/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <iostream>
#include <queue>
#include <string>
#include <vector>

#include <mesos/v1/mesos.hpp>
#include <mesos/v1/resources.hpp>
#include <mesos/v1/scheduler.hpp>

#include <process/defer.hpp>
#include <process/delay.hpp>
#include <process/process.hpp>
#include <process/protobuf.hpp>

#include <stout/os.hpp>
#include <stout/hashset.hpp>
#include <stout/stringify.hpp>

using namespace mesos::v1;

using std::cout;
using std::endl;
using std::queue;
using std::string;
using std::vector;

using mesos::v1::FrameworkInfo;
using mesos::v1::Resources;

using mesos::v1::scheduler::Call;
using mesos::v1::scheduler::Event;

const float CPUS_PER_TASK = 0.2;
const int32_t MEM_PER_TASK = 32;

class ExampleScheduler : public process::Process<ExampleScheduler>
{
public:
  ExampleScheduler(
      const FrameworkInfo& _framework,
      const string& master,
      const uint64_t _num_tasks)
    : framework(_framework),
      mesos(
          master,
          process::defer(self(), &Self::connected),
          process::defer(self(), &Self::disconnected),
          process::defer(self(), &Self::received, lambda::_1)),
      num_tasks(_num_tasks),
      tasksLaunched(0),
      state(INITIALIZING) {}

  ~ExampleScheduler() {}

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

    if (framework.has_id()) {
      subscribe->set_force(true);
    }

    mesos.send(call);

    process::delay(
        Seconds(1),
        self(),
        &Self::connected);
  }

  void disconnected()
  {
    state = DISCONNECTED;
  }

  void received(queue<Event> events)
  {
    while (!events.empty()) {
      Event event = events.front();
      events.pop();

      switch (event.type()) {
        case Event::SUBSCRIBED: {
          cout << "Subscribed with ID '" << framework.id() << "'" << endl;
          framework.mutable_id()->CopyFrom(event.subscribed().framework_id());
          state = SUBSCRIBED;
          break;
        }

        case Event::OFFERS: {
          cout << endl << "Received an OFFERS event" << endl;
          resourceOffers(google::protobuf::convert(event.offers().offers()));
          break;
        }

        case Event::RESCIND: {
          cout << endl << "Received a RESCIND event" << endl;
          break;
        }

        case Event::UPDATE: {
          cout << endl << "Received an UPDATE event" << endl;
          statusUpdate(event.update().status());
          break;
        }

        case Event::MESSAGE: {
          cout << endl << "Received a MESSAGE event" << endl;
          break;
        }

        case Event::FAILURE: {
          cout << endl << "Received a FAILURE event" << endl;
          break;
        }

        case Event::ERROR: {
          cout << endl << "Received an ERROR event: "
               << event.error().message() << endl;
          process::terminate(self());
          break;
        }

        case Event::HEARTBEAT: {
          cout << endl << "Received a HEARTBEAT event" << endl;
          break;
        }

        default: {
          EXIT(1) << "Received an UNKNOWN event";
        }
      }
    }
  }

private:
  void resourceOffers(const vector<Offer>& offers)
  {
    foreach (const Offer& offer, offers) {
      static const Resources TASK_RESOURCES = Resources::parse(
          "cpus:" + stringify(CPUS_PER_TASK) +
          ";mem:" + stringify(MEM_PER_TASK)).get();

      Call call;
      CHECK(framework.has_id());
      call.mutable_framework_id()->CopyFrom(framework.id());

      // Only one sleeper should occupy a single agent.
      if (agent_beds.size() < num_tasks &&
          !agent_beds.contains(offer.agent_id())) {
        TaskInfo task;
        task.mutable_task_id()->set_value(stringify(tasksLaunched));
        task.set_name("Sleeper Agent " + stringify(tasksLaunched++));
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

        agent_beds.insert(offer.agent_id());
      } else {
        // Don't hog offers.
        call.set_type(Call::DECLINE);

        Call::Decline* decline = call.mutable_decline();
        decline->add_offer_ids()->CopyFrom(offer.id());
      }

      mesos.send(call);
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

      mesos.send(call);
    }

    // Keep track of which sleepers are still sleeping.
    if (status.state() == TASK_FINISHED ||
        status.state() == TASK_LOST ||
        status.state() == TASK_KILLED ||
        status.state() == TASK_FAILED) {
      agent_beds.erase(status.agent_id());
    }
  }

  void finalize()
  {
    Call call;
    CHECK(framework.has_id());
    call.mutable_framework_id()->CopyFrom(framework.id());
    call.set_type(Call::TEARDOWN);

    mesos.send(call);
  }

  FrameworkInfo framework;
  scheduler::Mesos mesos;
  uint64_t num_tasks;

  // Agents which currently hold a sleeper.
  hashset<AgentID> agent_beds;

  int tasksLaunched;

  enum State
  {
    INITIALIZING,
    SUBSCRIBED,
    DISCONNECTED,
  } state;
};


#define shift argc--, argv++
int main(int argc, char** argv)
{
  string master;
  string number;
  shift;
  while (true) {
    string s = argc > 0 ? argv[0] : "--help";
    if (argc > 1 && s == "--master") {
      master = argv[1];
      shift; shift;
    } else if (argc > 1 && s == "-n") {
      number = argv[1];
      shift; shift;
    } else {
      break;
    }
  }

  if (master.length() == 0 || number.length() == 0) {
    printf("Usage: example --master <host>:<port> -n <number>\n");
    exit(1);
  }

  char* end;
  long num_tasks = strtol(number.c_str(), &end, 10);
  if (errno == ERANGE || num_tasks <= 0) {
    printf("Expected integer greater than zero for -n\n");
    exit(1);
  }

  // Nothing special to say about this framework.
  FrameworkInfo framework;
  framework.set_user(os::user().get());
  framework.set_name("Inverse Offer Example Framework");

  ExampleScheduler* scheduler =
    new ExampleScheduler(framework, master, num_tasks);

  process::spawn(scheduler);
  process::wait(scheduler);

  delete scheduler;
  return EXIT_SUCCESS;
}
