# cython: language_level=3

from libcpp.string cimport string

import json


cdef extern from "window_bridge.hpp" namespace "jarvis::native":
    string list_windows_json(int limit) except +
    string active_window_json() except +
    string focus_window_json(const string& title_contains_utf8, long long hwnd_value) except +


cdef object _decode_payload(const string& payload):
    cdef bytes payload_bytes = payload.c_str()[:payload.size()]
    return json.loads(payload_bytes.decode("utf-8"))


def list_windows(int limit=120):
    return _decode_payload(list_windows_json(limit))


def active_window():
    return _decode_payload(active_window_json())


def focus_window(title_contains="", hwnd=0):
    cdef string encoded_title = str(title_contains or "").encode("utf-8")
    cdef long long hwnd_value = int(hwnd or 0)
    return _decode_payload(focus_window_json(encoded_title, hwnd_value))
