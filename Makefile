CXX      := g++
CXXFLAGS := -std=c++17 -Wall -Wextra -Wpedantic -pthread
SRC      := src/server.cpp
TARGET   := redforge

.PHONY: all release debug clean

all: $(TARGET)

release: CXXFLAGS += -O3
release: clean $(TARGET)

debug: CXXFLAGS += -g -fsanitize=address -fsanitize=undefined
debug: LDFLAGS  += -fsanitize=address -fsanitize=undefined
debug: clean $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS)

clean:
	rm -f $(TARGET) *.o
