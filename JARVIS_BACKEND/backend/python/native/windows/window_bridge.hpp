#pragma once

#include <string>

namespace jarvis::native {

std::string list_windows_json(int limit);
std::string active_window_json();
std::string focus_window_json(const std::string& title_contains_utf8, long long hwnd_value);
std::string focus_related_window_json(
    const std::string& query_utf8,
    const std::string& hint_query_utf8,
    const std::string& descendant_hint_query_utf8,
    const std::string& campaign_hint_query_utf8,
    const std::string& campaign_preferred_title_utf8,
    const std::string& preferred_title_utf8,
    const std::string& window_title_utf8,
    long long hwnd_value,
    long pid_value,
    int follow_descendant_chain_value,
    int max_descendant_focus_steps,
    int limit
);
std::string reacquire_related_window_json(
    const std::string& query_utf8,
    const std::string& hint_query_utf8,
    const std::string& descendant_hint_query_utf8,
    const std::string& campaign_hint_query_utf8,
    const std::string& campaign_preferred_title_utf8,
    const std::string& preferred_title_utf8,
    const std::string& window_title_utf8,
    long long hwnd_value,
    long pid_value,
    int limit
);
std::string trace_related_window_chain_json(
    const std::string& query_utf8,
    const std::string& hint_query_utf8,
    const std::string& descendant_hint_query_utf8,
    const std::string& campaign_hint_query_utf8,
    const std::string& campaign_preferred_title_utf8,
    const std::string& preferred_title_utf8,
    const std::string& window_title_utf8,
    long long hwnd_value,
    long pid_value,
    int limit
);

}  // namespace jarvis::native
