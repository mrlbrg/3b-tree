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
#endif
		throw std::runtime_error("Not in debug mode");
	}

	~Logger() { out.close(); }

	Logger &operator++() {
#ifndef NDEBUG
		level += "-";
		return *this;
#endif
		throw std::runtime_error("Not in debug mode");
	}
	Logger &operator--() {
#ifndef NDEBUG
		assert(level.size() > 0);
		level.pop_back();
		return *this;
#endif
		throw std::runtime_error("Not in debug mode");
	}

  private:
	std::ofstream out{"/Users/marlenebargou/dev/3b-tree/log.txt"};

	std::string level = "";
};

extern Logger logger;

} // namespace bbbtree