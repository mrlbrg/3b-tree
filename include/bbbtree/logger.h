#pragma once

#include <cassert>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>

namespace bbbtree {

class Logger {

#ifndef PROJECT_SOURCE_DIR
#error "PROJECT_SOURCE_DIR not defined!"
#endif
	const std::filesystem::path log_file =
		std::filesystem::path(PROJECT_SOURCE_DIR) / "log.txt";

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

	inline void clear() {
		out.close();
		out.open(log_file, std::ofstream::trunc);
	}

  private:
	std::ofstream out{log_file, std::ofstream::trunc};

	std::string level = "";
};

extern Logger logger;

} // namespace bbbtree