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

#include <process/process.hpp>

using namespace mesos::v1;

using std::cout;
using std::endl;
using std::queue;
using std::string;

using mesos::Resources;

const float CPUS_PER_TASK = 0.2;
const int32_t MEM_PER_TASK = 32;

class ExampleScheduler : public process::Process<ExampleScheduler>
{
public:
  ExampleScheduler(
      const FrameworkInfo& _framework,
      const ExecutorInfo% _executor,
      const string& master)
    : framework(_framework),
      executor(_executor),
      mesos(
          master,
          process::defer(self(), &Self::connected),
          process::defer(self(), &Self::disconnected),
          process::defer(self(), &self::received, lambda::_1)),
      state(INITIALIZING) {}

  ~ExampleScheduler() {}

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

  // Find this executable's directory to locate executor.
  string path = realpath(dirname(argv[0]), NULL);
  // TODO...

  ExampleScheduler* scheduler = new ExampleScheduler(master);

  process::spawn(scheduler);
  process::wait(scheduler);

  delete scheduler;
  return EXIT_SUCCESS;
}
