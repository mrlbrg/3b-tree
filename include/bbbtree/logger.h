#pragma once

#include <cassert>
#include <fstream>
#include <stdexcept>
#include <string>

namespace bbbtree {

class Logger {

  public:
	void log(const std::string &message) {
#ifndef NDEBUG
		assert(out.is_open());
		out << level << message << std::endl;
#else
		throw std::runtime_error("Not in debug mode");
#endif
	}

	~Logger() { out.close(); }

	Logger &operator++() {
#ifndef NDEBUG
		level += "-";
		return *this;
#else
		throw std::runtime_error("Not in debug mode");
#endif
	}
	Logger &operator--() {
#ifndef NDEBUG
		assert(level.size() > 0);
		level.pop_back();
		return *this;
#else
		throw std::runtime_error("Not in debug mode");
#endif
	}

  private:
	std::ofstream out{"/Users/marlenebargou/dev/3b-tree/log.txt"};

	std::string level = "";
};

extern Logger logger;

} // namespace bbbtree