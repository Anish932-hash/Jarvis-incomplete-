#pragma once

#include <string>

namespace jarvis::native {

std::string list_windows_json(int limit);
std::string active_window_json();
std::string focus_window_json(const std::string& title_contains_utf8, long long hwnd_value);

}  // namespace jarvis::native
