CXX = g++
CXXFLAGS = -Wall -g -O2 -pthread -std=c++0x
LDFLAGS += -lmesos -lpthread -lprotobuf
CXXCOMPILE = $(CXX) $(INCLUDES) $(CXXFLAGS) -c -o $@
CXXLINK = $(CXX) $(INCLUDES) $(CXXFLAGS) -o $@

default: all
all: example

HEADERS =


%: %.cpp $(HEADERS)
	$(CXXLINK) $< $(LDFLAGS)

clean:
	(rm -f example)
