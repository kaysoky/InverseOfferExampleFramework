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

#include <mesos/v1/mesos.hpp>
#include <mesos/v1/resources.hpp>
#include <mesos/v1/scheduler.hpp>

#include <process/defer.hpp>
#include <process/delay.hpp>
#include <process/process.hpp>

using namespace mesos::v1;

using std::cout;
using std::endl;
using std::queue;
using std::string;

using mesos::v1::ExecutorInfo;
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
      const ExecutorInfo& _executor,
      const string& master)
    : framework(_framework),
      executor(_executor),
      mesos(
          master,
          process::defer(self(), &Self::connected),
          process::defer(self(), &Self::disconnected),
          process::defer(self(), &Self::received, lambda::_1)),
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

          // TODO: Do something with the offer(s).
          break;
        }

        case Event::RESCIND: {
          cout << endl << "Received a RESCIND event" << endl;
          break;
        }

        case Event::UPDATE: {
          cout << endl << "Received an UPDATE event" << endl;
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

  // TODO: Write the 3 lambdas and a bunch of other things.

private:
  FrameworkInfo framework;
  const ExecutorInfo executor;
  scheduler::Mesos mesos;

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
  shift;
  while (true) {
    string s = argc > 0 ? argv[0] : "--help";
    if (argc > 1 && s == "--master") {
      master = argv[1];
      shift; shift;
    } else {
      break;
    }
  }

  if (master.length() == 0) {
    printf("Usage: example --master <ip>:<port>\n");
    exit(1);
  }

  // Nothing special to say about this framework.
  FrameworkInfo framework;
  framework.set_user(""); // Have Mesos fill in the current user.
  framework.set_name("Inverse Offer Example Framework");

  // The default executor is good enough.
  ExecutorInfo executor;
  executor.mutable_executor_id()->set_value("default");

  ExampleScheduler* scheduler = new ExampleScheduler(framework, executor, master);

  process::spawn(scheduler);
  process::wait(scheduler);

  delete scheduler;
  return EXIT_SUCCESS;
}
