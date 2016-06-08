// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <iostream>
#include <string>
#include <vector>
#include <queue>

#include <mesos/v1/mesos.hpp>
#include <mesos/v1/resources.hpp>
#include <mesos/v1/scheduler.hpp>

#include <process/clock.hpp>
#include <process/defer.hpp>
#include <process/delay.hpp>
#include <process/http.hpp>
#include <process/owned.hpp>
#include <process/process.hpp>
#include <process/protobuf.hpp>
#include <process/time.hpp>

#include <process/metrics/counter.hpp>
#include <process/metrics/gauge.hpp>
#include <process/metrics/metrics.hpp>

#include <stout/check.hpp>
#include <stout/exit.hpp>
#include <stout/flags.hpp>
#include <stout/foreach.hpp>
#include <stout/hashmap.hpp>
#include <stout/json.hpp>
#include <stout/lambda.hpp>
#include <stout/none.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/stringify.hpp>

using namespace mesos::v1;

using std::cerr;
using std::cout;
using std::endl;
using std::queue;
using std::string;
using std::vector;

using mesos::v1::scheduler::Call;
using mesos::v1::scheduler::Event;

using process::Clock;
using process::defer;

using process::http::OK;

using process::metrics::Gauge;
using process::metrics::Counter;


const float CPUS_PER_TASK = 0.2;
const int32_t MEM_PER_TASK = 32;


// Holds a sleeper and its bed.  Plus when the bed might be taken away.
struct Sleeper
{
  TaskID id;
  TimeInfo wake;
};


class ExampleScheduler : public process::Process<ExampleScheduler>
{
public:
  ExampleScheduler(
      const FrameworkInfo& _framework,
      const string& _master,
      const uint32_t _num_tasks)
    : framework(_framework),
      master(_master),
      num_tasks(_num_tasks),
      tasks_launched(0),
      state(INITIALIZING),
      uptime_secs(
          "inverse_offer_framework/uptime_secs",
          defer(this, &Self::_uptime_secs)),
      subscribed(
          "inverse_offer_framework/subscribed",
          defer(this, &Self::_subscribed)),
      offers_received(
          "inverse_offer_framework/offers_received"),
      inverse_offers_received(
          "inverse_offer_framework/inverse_offers_received"),
      sleepers_killed(
          "inverse_offer_framework/sleepers_killed"),
      sleepers_alarmed(
          "inverse_offer_framework/sleepers_alarmed"),
      current_sleepers(
          "inverse_offer_framework/current_sleepers",
          defer(this, &Self::_current_sleepers))
  {
    start_time = Clock::now();

    process::metrics::add(uptime_secs);
    process::metrics::add(subscribed);
    process::metrics::add(offers_received);
    process::metrics::add(inverse_offers_received);
    process::metrics::add(sleepers_killed);
    process::metrics::add(sleepers_alarmed);
    process::metrics::add(current_sleepers);
  }

  ~ExampleScheduler() {
    process::metrics::remove(uptime_secs);
    process::metrics::remove(subscribed);
    process::metrics::remove(offers_received);
    process::metrics::remove(inverse_offers_received);
    process::metrics::remove(sleepers_killed);
    process::metrics::remove(sleepers_alarmed);
    process::metrics::remove(current_sleepers);
  }

  // Continuously sends the `SUBSCRIBED` call until it is acknowledged.
  void connected()
  {
    if (state == SUBSCRIBED) {
      return;
    }

    Call call;
    if (framework.has_id()) {
      call.mutable_framework_id()->CopyFrom(framework.id());
    }
    call.set_type(Call::SUBSCRIBE);

    Call::Subscribe* subscribe = call.mutable_subscribe();
    subscribe->mutable_framework_info()->CopyFrom(framework);

    mesos->send(call);

    process::delay(
        Seconds(1),
        self(),
        &Self::connected);
  }

  void disconnected()
  {
    state = DISCONNECTED;

    // If we don't checkpoint, getting disconnected will kill all current tasks.
    if (!framework.checkpoint()) {
      sleepers.clear();
    }
  }

  void received(queue<Event> events)
  {
    while (!events.empty()) {
      Event event = events.front();
      events.pop();

      switch (event.type()) {
        case Event::SUBSCRIBED: {
          cout << "Received a SUBSCRIBED event" << endl;

          framework.mutable_id()->CopyFrom(event.subscribed().framework_id());
          state = SUBSCRIBED;

          cout << "Subscribed with ID '" << framework.id() << endl;
          break;
        }

        case Event::OFFERS: {
          cout << "Received an OFFERS event with "
               << event.offers().offers().size() << " offer(s)"
               << endl;

          offers_received += event.offers().offers().size();

          resourceOffers(google::protobuf::convert(event.offers().offers()));
          break;
        }

        case Event::INVERSE_OFFERS: {
          cout << "Received an INVERSE_OFFERS event with "
               << event.inverse_offers().inverse_offers().size()
               << " inverse offer(s)" << endl;

          inverse_offers_received +=
            event.inverse_offers().inverse_offers().size();

          inverseOffers(google::protobuf::convert(
              event.inverse_offers().inverse_offers()));
          break;
        }

        case Event::RESCIND: {
          cout << "Received a RESCIND event" << endl;
          break;
        }

        case Event::UPDATE: {
          cout << "Received an UPDATE event" << endl;

          statusUpdate(event.update().status());
          break;
        }

        case Event::MESSAGE: {
          cout << "Received a MESSAGE event" << endl;
          break;
        }

        case Event::FAILURE: {
          cout << "Received a FAILURE event" << endl;
          break;
        }

        case Event::ERROR: {
          cout << "Received an ERROR event: "
               << event.error().message() << endl;
          break;
        }

        case Event::HEARTBEAT: {
          cout << "Received a HEARTBEAT event" << endl;
          break;
        }

        default: {
          EXIT(1) << "Received an UNKNOWN event";
        }
      }
    }
  }

protected:
  virtual void initialize()
  {
    // We initialize the library here to ensure that callbacks are only invoked
    // after the process has spawned.
    mesos.reset(new scheduler::Mesos(
        master,
        mesos::ContentType::PROTOBUF,
        process::defer(self(), &Self::connected),
        process::defer(self(), &Self::disconnected),
        process::defer(self(), &Self::received, lambda::_1),
        None()));
  }

private:
  void resourceOffers(const vector<Offer>& offers)
  {
    // Find the riskiest sleeper (i.e. task running on the agent that is the
    // next to be maintained).  We'll see if we can migrate this sleeper.
    Option<AgentID> risky;
    foreachpair (const AgentID& bed, const Sleeper& snorelax, sleepers) {
      if (risky.isSome()) {
        if (snorelax.wake.nanoseconds() <
            sleepers[risky.get()].wake.nanoseconds()) {
          risky = bed;
        }
      } else if (snorelax.wake.nanoseconds() > 0) {
        risky = bed;
      }
    }

    foreach (const Offer& offer, offers) {
      static const Resources TASK_RESOURCES = Resources::parse(
          "cpus:" + stringify(CPUS_PER_TASK) +
          ";mem:" + stringify(MEM_PER_TASK)).get();

      Call call;
      CHECK(framework.has_id());
      call.mutable_framework_id()->CopyFrom(framework.id());

      // Check if this offer is big enough.
      if (!Resources(offer.resources()).flatten().contains(TASK_RESOURCES)) {
        call.set_type(Call::DECLINE);

        Call::Decline* decline = call.mutable_decline();
        decline->add_offer_ids()->CopyFrom(offer.id());
        decline->mutable_filters()->set_refuse_seconds(600);

        mesos->send(call);
        continue;
      }

      // Are there already `num_task` sleepers?
      // Have `num_task` sleepers is this framework's SLA.
      // More sleepers takes priority over dealing with maintenance.
      bool need_more_sleep = sleepers.size() < num_tasks;

      // Is there a better bed available for some sleeper?
      // NOTE: This is mutually exclusive with `need_more_sleep`.
      bool can_migrate = !need_more_sleep && risky.isSome() &&
          (!offer.has_unavailability() ||
            offer.unavailability().start().nanoseconds() >
            sleepers[risky.get()].wake.nanoseconds());

      // Only one sleeper should occupy a single agent.
      if ((can_migrate || need_more_sleep) &&
          !sleepers.contains(offer.agent_id())) {
        // "Wake" the old task first.
        // We'll wait for a status update before modifying `sleepers`.
        if (can_migrate) {
          cout << "Migrating task " << sleepers[risky.get()].id << endl;

          Call wakeup;
          wakeup.mutable_framework_id()->CopyFrom(framework.id());
          wakeup.set_type(Call::KILL);

          Call::Kill* kill = wakeup.mutable_kill();
          kill->mutable_task_id()->CopyFrom(sleepers[risky.get()].id);
          kill->mutable_agent_id()->CopyFrom(risky.get());

          mesos->send(wakeup);

          // Keep track of this sleeper in another map.
          migrating[risky.get()] = sleepers[risky.get()];
          sleepers.erase(risky.get());

          // We'll just migrate one task per round of offers.
          risky = None();
        }

        TaskInfo task;
        task.mutable_task_id()->set_value(stringify(tasks_launched));
        task.set_name("Sleeper Agent " + stringify(tasks_launched++));
        task.mutable_agent_id()->MergeFrom(offer.agent_id());
        task.mutable_resources()->CopyFrom(TASK_RESOURCES);
        task.mutable_command()->set_value(
            "while [ true ]; do echo ZZZzzz...; sleep 5; done");

        call.set_type(Call::ACCEPT);

        Call::Accept* accept = call.mutable_accept();
        accept->add_offer_ids()->CopyFrom(offer.id());

        Offer::Operation* operation = accept->add_operations();
        operation->set_type(Offer::Operation::LAUNCH);
        operation->mutable_launch()->add_task_infos()->CopyFrom(task);

        // Save the new sleeper.
        Sleeper snorelax;
        snorelax.id = task.task_id();
        if (offer.has_unavailability()) {
          snorelax.wake = offer.unavailability().start();
        }
        sleepers[offer.agent_id()] = snorelax;

      } else {
        // Don't hog offers.
        call.set_type(Call::DECLINE);

        Call::Decline* decline = call.mutable_decline();
        decline->add_offer_ids()->CopyFrom(offer.id());
        decline->mutable_filters()->set_refuse_seconds(600);
      }

      mesos->send(call);
    }
  }


  void inverseOffers(const vector<InverseOffer>& offers)
  {
    foreach (const InverseOffer& offer, offers) {
      if (!sleepers.contains(offer.agent_id())) {
        cout << "Inverse Offer received for Agent " << offer.agent_id()
             << " which does not hold a sleeper." << endl;
        continue;
      }

      // Take note of any agents that are scheduled for maintenance.
      sleepers[offer.agent_id()].wake = offer.unavailability().start();

      // TODO: Demonstrate some semantics for declining inverse offers.
      // This framework always accepts inverse offers.
      Call call;
      CHECK(framework.has_id());
      call.mutable_framework_id()->CopyFrom(framework.id());

      call.set_type(Call::ACCEPT);
      Call::Accept* accept = call.mutable_accept();
      accept->add_offer_ids()->CopyFrom(offer.id());

      mesos->send(call);
    }
  }


  void statusUpdate(const TaskStatus& status)
  {
    cout << "Task " << status.task_id() << " is in state " << status.state();

    if (status.has_message()) {
      cout << " with message '" << status.message() << "'";
    }
    cout << endl;

    if (status.has_uuid()) {
      Call call;
      CHECK(framework.has_id());
      call.mutable_framework_id()->CopyFrom(framework.id());
      call.set_type(Call::ACKNOWLEDGE);

      Call::Acknowledge* ack = call.mutable_acknowledge();
      ack->mutable_agent_id()->CopyFrom(status.agent_id());
      ack->mutable_task_id()->CopyFrom(status.task_id());
      ack->set_uuid(status.uuid());

      mesos->send(call);
    }

    // Keep track of which sleepers are still sleeping.
    // These are un-expected terminal states.
    if (status.state() == TASK_FINISHED ||
        status.state() == TASK_LOST ||
        status.state() == TASK_FAILED ||
        status.state() == TASK_ERROR) {
      ++sleepers_alarmed;

      migrating.erase(status.agent_id());
    }

    // This is the only expected terminal state.
    if (status.state() == TASK_KILLED) {
      ++sleepers_killed;

      migrating.erase(status.agent_id());
    }
  }

  void finalize()
  {
    if (state == SUBSCRIBED) {
      Call call;
      CHECK(framework.has_id());
      call.mutable_framework_id()->CopyFrom(framework.id());
      call.set_type(Call::TEARDOWN);

      mesos->send(call);
    }
  }

  FrameworkInfo framework;
  const string master;
  const uint32_t num_tasks;

  // Agents which currently hold a sleeper.
  hashmap<AgentID, Sleeper> sleepers;
  hashmap<AgentID, Sleeper> migrating;

  int tasks_launched;

  process::Owned<scheduler::Mesos> mesos;

  enum State
  {
    INITIALIZING = 0,
    SUBSCRIBED = 1,
    DISCONNECTED = 2
  } state;

  /************
   * Metrics! *
   ************/

  process::Time start_time;
  double _uptime_secs()
  {
    return (Clock::now() - start_time).secs();
  }

  double _subscribed()
  {
    return state == SUBSCRIBED ? 1 : 0;
  }

  double _current_sleepers()
  {
    return sleepers.size() + migrating.size();
  }

  process::metrics::Gauge uptime_secs;
  process::metrics::Gauge subscribed;

  process::metrics::Counter offers_received;
  process::metrics::Counter inverse_offers_received;

  // The only expected terminal state is TASK_KILLED.
  // Other terminal states are considered incorrect.
  process::metrics::Counter sleepers_killed;
  process::metrics::Counter sleepers_alarmed;

  process::metrics::Gauge current_sleepers;
};


class Flags : public flags::FlagsBase
{
public:
  Flags()
  {
    add(&role,
        "role",
        "Role to use when registering.",
        "*");

    add(&master,
        "master",
        "Master to connect to.",
        [](const Option<string>& value) -> Option<Error> {
          if (value.isNone()) {
            return Error("Missing --master");
          }

          return None();
        });

    add(&num_tasks,
        "num_tasks",
        "Number of sleeps to start.",
        1,
        [](int value) -> Option<Error> {
          if (value <= 0) {
            return Error("Expected --num_tasks greater than zero");
          }

          return None();
        });

    add(&checkpoint,
        "checkpoint",
        "Whether this framework should be checkpointed.",
        false);
  }

  string role;
  Option<string> master;
  int num_tasks;
  bool checkpoint;
};


int main(int argc, char** argv)
{
  Flags flags;
  Try<flags::Warnings> load = flags.load(None(), argc, argv);

  if (load.isError()) {
    cerr << flags.usage(load.error()) << endl;
    EXIT(1);
  }

  // Nothing special to say about this framework.
  FrameworkInfo framework;
  framework.set_user(os::user().get());
  framework.set_name("inverse-offer-example-framework");
  framework.set_role(flags.role);
  framework.set_checkpoint(flags.checkpoint);

  process::Owned<ExampleScheduler> scheduler(
      new ExampleScheduler(framework, flags.master.get(), flags.num_tasks));

  process::spawn(scheduler.get());
  process::wait(scheduler.get());

  return EXIT_SUCCESS;
}
