#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <thread>
#include <vector>
#include <stdlib.h>
#include <algorithm>
#include <cctype>
#include <sstream>
#include <unordered_map>
#include <map>
#include <shared_mutex>
#include <mutex>
#include <chrono>
#include <functional>
#include <deque>
#include <condition_variable>
#include <unordered_set>
#include <string_view>
#include <charconv>
#include <fstream>
#include <cstdint>
#include <climits>
#include <optional>

template<typename T>
std::optional<T> safe_parse(std::string_view sv) {
    T val{};
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), val);
    if(ec != std::errc{} || ptr != sv.data() + sv.size()) return std::nullopt;
    return val;
}

class RDBReader{
  std::ifstream file;

  public:
    explicit RDBReader(const std::string& filepath) : file(filepath, std::ios::binary){}

    bool is_open() const{
      return file.is_open();
    }

    //read single unsigned byte
    bool read_byte(uint8_t& b){
      char ch;
      if(file.get(ch)){
        b = static_cast<uint8_t>(ch);
        return true;
      }
      return false;
    }

    //read exact N raw bytes into a buffer
    bool read_exact(char * dest, int len){
      file.read(dest, len);
      return file.gcount() == static_cast<std::streamsize>(len);
    }

    //decode Redis lenght encoding
    bool read_length(uint32_t& length, bool& is_special_int){
      uint8_t first_byte;
      if(!read_byte(first_byte)) return false;
      uint8_t flag = (first_byte >> 6);
      is_special_int = false;

      if(flag == 0b00){
        length = first_byte & 0x3F;
        return true;
      } else if(flag == 0b01){
        uint8_t next_byte;
        if(!read_byte(next_byte)) return false;
        length = ((first_byte & 0x3F) << 8) | next_byte;
        return true;
      } else if(flag == 0b10){//32 big endian
        char buf[4];
        if(!read_exact(buf, 4)) return false;
        length =  (static_cast<uint8_t>(buf[0]) << 24) |
                  (static_cast<uint8_t>(buf[1]) << 16) |
                  (static_cast<uint8_t>(buf[2]) << 8) |
                  (static_cast<uint8_t>(buf[3])) ;
        return true;
      } else{
        //flag is 0b11 special integer string encoding
        is_special_int = true;
        length = first_byte & 0x3F;
        return true;
      }

      return false;
    }

    //read a redis string (handles regular string and integer-encoded strings)
    bool read_string(std::string& out_str){
      uint32_t len = 0;
      bool is_special = false;
      if(!read_length(len, is_special)) return false;

      if(is_special){
        if(len == 0){//8 bit integer
          //8 bit integer
          uint8_t val;
          if(!read_byte(val)) return false;
          out_str = std::to_string(static_cast<int8_t>(val));
        } else if(len == 1){//16 bit integer (little endian)
          uint16_t val;
          if(!read_exact(reinterpret_cast<char*>(&val), 2)) return false;
          out_str = std::to_string(val);
        } else if(len == 2){//32 bit integer (little endian)
          uint32_t val;
          if(!read_exact(reinterpret_cast<char*>(&val), 4)) return false;
          out_str = std::to_string(val);
        }
        
        return true;
      }

      //standard string with regular byte length
      out_str.resize(len);
      return read_exact(&out_str[0], len);
    }
};

struct ClientState{
  bool in_transaction = false;
  bool watched_keys_modified = false;
  std::vector<std::vector<std::string>> transaction_queue;
  std::unordered_set<std::string> watched_keys;
};

struct Value{
  std::string value;
  std::chrono::time_point<std::chrono::steady_clock> expiry_time;
  bool has_expiry = false;
};

struct StreamID{
  uint64_t ms = 0;
  uint64_t seq = 0;

  bool operator<(const StreamID& o) const {
    return (ms != o.ms) ? ms < o.ms : seq < o.seq;
  }

  bool operator<=(const StreamID& o) const {
    return (ms != o.ms) ? ms < o.ms : seq <= o.seq;
  }

  bool operator>(const StreamID& o) const{
    return (ms != o.ms) ? ms > o.ms : seq > o.seq;
  }

  bool operator==(const StreamID& o) const{
    return ms == o.ms && seq == o.seq;
  }

  std::string to_string() const{
    return std::to_string(ms) + "-" + std::to_string(seq);
  }

  static bool parse_explicit_stream_id(const std::string& str, StreamID& out, bool is_end){
    if(!is_end && str == "-"){
      out.ms = out.seq = 0;
      return true;
    }

    if(is_end && str == "+"){
      out.ms = out.seq = UINT64_MAX;
      return true;
    }

    size_t dash = str.find('-');
    if(dash == 0 || dash + 1 == str.size()){
      return false;
    }

    if(dash == std::string::npos){
        auto ms = safe_parse<uint64_t>(str);
        if(!ms) return false;
        out.ms = *ms;
        out.seq = is_end ? UINT64_MAX : 0;
      } else{
        auto ms = safe_parse<uint64_t>(str.substr(0, dash));
        auto seq = safe_parse<uint64_t>(str.substr(dash+1));
        if(!ms || !seq) return false;
        out.ms = *ms;
        out.seq = *seq;
      }
      return true;
  }

};

using StreamFields = std::vector<std::pair<std::string, std::string>>;
using StreamLog = std::map<StreamID, StreamFields>;

thread_local std::string *g_reponse_trap = nullptr;

std::unordered_map<std::string, Value> kv_store;
std::shared_mutex kv_mutex;

std::unordered_map<std::string, std::deque<std::string>> list_store;
std::shared_mutex list_mutex;
std::condition_variable_any cv_list;

std::unordered_map<std::string, StreamLog> stream_store;
std::shared_mutex stream_mutex;

inline std::unordered_map<std::string, std::unordered_set<ClientState*>> g_watched_keys_registry;
inline std::mutex g_watch_mutex;

std::string g_role = "master";
std::string g_master_host = "";
int g_master_port = 0;

std::string g_master_replid = "8371b4fb1155b71f4a04d3f1bc3e18c4a990aeeb";
long long g_master_repl_offset = 0;

//connected replcis list
std::vector<int> g_replicas;
std::mutex g_replicas_mutex;

std::unordered_map<int, long long> g_replica_offsets;
std::mutex g_replica_offsets_mutex;
std::condition_variable g_replica_cv;

const std::string EMPTY_RDB_HEX = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473e040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

//global persistence configurations
std::string g_rdb_dir = "";
std::string g_rdb_filename = "";

std::string hex_to_bytes(const std::string& hex){
  std::string bytes;
  for(size_t i = 0 ; i < hex.length() ; i += 2){
    std::string byteString = hex.substr(i, 2);
    char byte = strtol(byteString.c_str(), nullptr, 16);
    bytes.push_back(byte);
  }
  return bytes;
}

//Forward declarations
void send_response(int fd, const std::string& resp);
void handle_ping(int fd, const std::vector<std::string>& args);
void handle_echo(int fd, const std::vector<std::string>& args);
void handle_set(int fd, const std::vector<std::string>& args);
void handle_get(int fd, const std::vector<std::string>& args);
void handle_rpush(int fd, const std::vector<std::string>& args);
void handle_lrange(int fd, const std::vector<std::string>& args);
void handle_lpush(int fd, const std::vector<std::string>& args);
void handle_llen(int fd, const std::vector<std::string>& args);
void handle_lpop(int fd, const std::vector<std::string>& args);
void handle_blpop(int fd, const std::vector<std::string>& args);
void handle_type(int fd, const std::vector<std::string>& args);
void handle_xadd(int fd, const std::vector<std::string>& args);
void handle_xrange(int fd, const std::vector<std::string>& args);
void handle_xread(int fd, const std::vector<std::string>& args);
void handle_incr(int fd, const std::vector<std::string>& args);
void handle_watch(int fd, const std::vector<std::string>& args, ClientState& state);
void handle_unwatch(int fd, ClientState& state);
void handle_info(int fd, const std::vector<std::string>& args);
void handle_replconf(int fd, const std::vector<std::string>& args);
void handle_psync(int fd, const std::vector<std::string>& args);
void handle_wait(int fd, const std::vector<std::string>& args);
void propagate_to_replicas(const std::string& raw_resp_cmd);
void handle_config(int fd, const std::vector<std::string>& args);
void handle_keys(int fd, const std::vector<std::string>& args);
void handle_command(int fd, const std::vector<std::string>& args);
void dispatch_command(int fd, const std::vector<std::string>& args);
void touch_key(const std::string& key);
void unwatch_all(ClientState& state);

static const std::unordered_map<std::string, std::function<void(int, const std::vector<std::string>&)>> handlers = {
    {"ping", handle_ping},
    {"echo", handle_echo},
    {"set", handle_set},
    {"get", handle_get},
    {"rpush", handle_rpush},
    {"lrange", handle_lrange},
    {"lpush", handle_lpush},
    {"llen", handle_llen},
    {"lpop", handle_lpop},
    {"blpop", handle_blpop},
    {"type", handle_type},
    {"xadd", handle_xadd},
    {"xrange", handle_xrange},
    {"xread", handle_xread},//blocking reads support is not there
    {"incr", handle_incr},
    {"info", handle_info},
    {"replconf", handle_replconf},
    {"psync", handle_psync},
    {"wait", handle_wait},
    {"config", handle_config},
    {"keys", handle_keys},
    {"command", handle_command},
  };

std::string resp_ok() {return "+OK\r\n"; }
std::string resp_pong() {return "+PONG\r\n"; }
std::string resp_simple(const std::string& s){
  return "+"+s+"\r\n";
}
std::string resp_bulk(const std::string& s) {
  return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
}
std::string resp_null() {return "$-1\r\n";}
std::string resp_integer(long long n){
  return ":" + std::to_string(n) + "\r\n";
}
std::string resp_array(const std::vector<std::string>& response_array) {
  std::string s = "*"+std::to_string(response_array.size())+"\r\n";
  for(auto& it : response_array) s += resp_bulk(it);
  return s;
}

std::string resp_error(const std::string& msg){
  return "-ERR " + msg + "\r\n";
}

void send_response(int fd, const std::string& resp){

  if(fd < 0) return ; //replica replication loop

  if(g_reponse_trap != nullptr){
    *g_reponse_trap += resp;
    return;
  }

  size_t total = 0;
  while(total < resp.size()){
    ssize_t sent = send(fd, resp.c_str() + total, resp.size() - total, 0);
    if(sent <= 0) break;
    total += sent;
  }
}

void handle_ping(int fd, const std::vector<std::string>& args){
  if(args.size() == 1) send_response(fd, resp_pong());
  else send_response(fd, resp_bulk(args[1]));
}

void handle_echo(int fd, const std::vector<std::string>& args){
  if(args.size() < 2){
    send_response(fd, resp_error("wrong number of arguments"));
    return;
  }
  send_response(fd, resp_bulk(args[1]));
}

void handle_set(int fd, const std::vector<std::string>& args){
  if(args.size() < 3){
    send_response(fd, resp_error("wrong number of arguments"));
    return;
  }

  std::string key = args[1];
  std::string value = args[2];
  Value entry;
  entry.value = value;

  if(args.size() >= 5){
    std::string expiry = args[3];
    for(auto& ch : expiry) ch = tolower(ch);
    auto units = safe_parse<long long>(args[4]);
    if(!units || *units < 0){
      send_response(fd, resp_error("invalid expire time"));
      return;
    }
    if(expiry == "ex"){
      *units *= 1000;
    } else if(expiry != "px"){
      send_response(fd, resp_error("invalid expire time"));
      return;
    }

    entry.expiry_time = std::chrono::steady_clock::now() + std::chrono::milliseconds(*units);
    entry.has_expiry = true;
  }

  {
    //unique_lock locks everyone out until this block finishes
    std::unique_lock lock(kv_mutex);
    kv_store[key] = entry;
  } //lock automatically releases here

  touch_key(key);

  if(g_role == "master") propagate_to_replicas(resp_array(args));

  send_response(fd, resp_ok());
}

void handle_get(int fd, const std::vector<std::string>& args) {
    if (args.size() < 2) {
        send_response(fd, resp_error("wrong number of arguments"));
        return;
    }

    std::string key = args[1];
    std::string response;
    bool needs_deletion = false;

    // Fast Path: Concurrent Read via shared_lock
    {
        std::shared_lock lock(kv_mutex);
        auto it = kv_store.find(key);
        if (it == kv_store.end()) {
            response = resp_null();
        } else if (it->second.has_expiry && std::chrono::steady_clock::now() >= it->second.expiry_time) {
            needs_deletion = true;
        } else {
            response = resp_bulk(it->second.value);
        }
    }

    // Slow Path: Exclusive Write Lock for Lazy Eviction
    if (needs_deletion) {
        std::unique_lock lock(kv_mutex);
        auto it = kv_store.find(key);
        if (it == kv_store.end()) {
            response = resp_null();
        } else if (it->second.has_expiry && std::chrono::steady_clock::now() >= it->second.expiry_time) {
            kv_store.erase(it);
            response = resp_null();
        } else {
            response = resp_bulk(it->second.value);
        }
    }

    send_response(fd, response);
}

void handle_rpush(int fd, const std::vector<std::string>& args){
  if(args.size() < 3){
    send_response(fd, resp_error("wrong number of arguments"));
    return;
  }
  //first find the list
  int len = 0;
  const std::string& key = args[1];

  {
    std::unique_lock lock(list_mutex);
    if(list_store.find(args[1]) == list_store.end()){
      list_store.insert({args[1], {}});
    }

    std::deque<std::string>& v = list_store[args[1]];

    for(size_t i = 2 ; i < args.size() ; i++){
      v.push_back(args[i]);
    }

    len  = list_store[args[1]].size();
  }

  cv_list.notify_all();

  touch_key(key);
  if(g_role == "master") propagate_to_replicas(resp_array(args));
  send_response(fd, resp_integer(len));
}

void handle_lrange(int fd, const std::vector<std::string>& args){
  if(args.size() < 4){
    send_response(fd, resp_error("wrong number of arguments"));
    return;
  }

  auto l = safe_parse<long long>(args[2]);
  auto r = safe_parse<long long>(args[3]);
  if(!l || !r){
    send_response(fd, resp_error("invalid indices"));
    return;
  }

  std::vector<std::string> response_array;

  //shared lock only for reading
  {
    std::shared_lock lock(list_mutex);
    auto it = list_store.find(args[1]);
    if(it != list_store.end()){
      std::deque<std::string>& v = it->second;
      long long sz = v.size();

      //possible only when l <= r
      if(*l < 0) *l = *l + sz;
      if(*r < 0) *r = *r + sz;

      if(*l < 0) *l = 0;
      if(*r >= sz) *r = sz-1;


      if(*l < sz && *l <= *r){
        while(*l <= *r){
          response_array.push_back(v[*l]);
          (*l)++;
        }
      } 
    }

  }
  send_response(fd, resp_array(response_array));
}

void handle_lpush(int fd, const std::vector<std::string>& args){
  if(args.size() < 3){
    send_response(fd, resp_error("wrong number of arguments"));
    return;
  }
  //first find the list
  int len = 0;
  const std::string& key = args[1];
  {
    std::unique_lock lock(list_mutex);
    if(list_store.find(args[1]) == list_store.end()){
      list_store.insert({args[1], {}});
    }

    std::deque<std::string>& v = list_store[args[1]];

    for(size_t i = 2 ; i < args.size() ; i++){
      v.push_front(args[i]);
    }

    len  = list_store[args[1]].size();
  }

  cv_list.notify_all();
  touch_key(key);
  if(g_role == "master") propagate_to_replicas(resp_array(args));
  send_response(fd, resp_integer(len));
}

void handle_llen(int fd, const std::vector<std::string>& args){
  if(args.size() < 2){
    send_response(fd, resp_error("wrong number of arguments"));
    return;
  }

  int len;

  {
    std::shared_lock lock(list_mutex);
    auto it = list_store.find(args[1]);
    if(it != list_store.end()) len = (it -> second).size();
    else len = 0;
  }

  send_response(fd, resp_integer(len));
}

void handle_lpop(int fd, const std::vector<std::string>& args){
  if(args.size() < 2){
    send_response(fd, resp_error("wrong number of arguments"));
    return;
  }

  const std::string& key = args[1];

  bool has_count = (args.size() >= 3);
  long long count = 1;

  if(has_count){
    auto parsed = safe_parse<long long>(args[2]);
    if(!parsed){
      send_response(fd, resp_error("value is not an integer"));
      return;
    }
    if(*parsed < 0){
      send_response(fd, resp_error("value should be positive"));
      return;
    }
    count = *parsed;
  }

  std::string response;
  {
    std::unique_lock lock(list_mutex);
    //find the key
    auto it = list_store.find(args[1]);
    if(it == list_store.end()) response = resp_null();
    else{
      std::deque<std::string>& v = it -> second;
      count = std::min(count, (long long)v.size());
      if(v.empty()) response = resp_null();
      else{
        if(args.size() == 2){
          response = resp_bulk(v.front());
          v.pop_front();
        } else{
          std::vector<std::string> response_array;
          for(long long i = 0 ; i < count ; i++){
            response_array.push_back(v.front());
            v.pop_front();
          }
          response = resp_array(response_array);
        }

        if(v.empty()) list_store.erase(it);
      }
    }
  }
  touch_key(key);
  if(g_role == "master") propagate_to_replicas(resp_array(args));
  send_response(fd, response);
}

void handle_blpop(int fd, const std::vector<std::string>& args){
  if(args.size() < 3){
    send_response(fd, resp_error("wrong number of arguments"));
    return;
  }

  double timeout;

  try{
    timeout = stod(args[2]);
    if(timeout < 0.0){
      send_response(fd, resp_error("timeout should be non-negative"));
      return;
    }
  }catch(...){
    send_response(fd, resp_error("timeout should be a float"));
    return;
  }

  std::string matched_key;
  std::string popped_val;
  bool element_found = false;

  auto check_keys = [&](){
    for(size_t i = 1 ; i < args.size()-1 ; i++){
      auto it = list_store.find(args[i]);
      if(it != list_store.end() && !it->second.empty()) return true;
    }
    return false;
  };

  {
    std::unique_lock lock(list_mutex);

    if(timeout == 0){
      cv_list.wait(lock, check_keys);
      //if this is false it unlocks list_mutex and puts thread to sleep
      //wakes up upon notify and checks again it true move forward
      element_found = true;
    } else{
      element_found = cv_list.wait_for(lock, std::chrono::duration<double>(timeout), check_keys);
    }

    if(element_found){
      for(size_t i = 1 ; i < args.size()-1 ; i++){
        auto it = list_store.find(args[i]);
        if(it != list_store.end() && !it->second.empty()){
          matched_key = args[i];
          popped_val = it->second.front();
          it->second.pop_front();

          if(it->second.empty()){
            list_store.erase(it);
          }
          break;
        }
      }
    }
  }

  if(element_found){
    touch_key(matched_key);
    if(g_role == "master") propagate_to_replicas(resp_array(args));
    send_response(fd, resp_array({matched_key, popped_val}));
  } else{
    send_response(fd, resp_null());
  }


}

void handle_type(int fd, const std::vector<std::string>& args){
  if(args.size() < 2){
    send_response(fd, resp_error("wrong number of arguments"));
    return;
  }

  const std::string& key = args[1];
  std::string type_result = "none";
  {
    std::shared_lock lock(kv_mutex);
    auto it = kv_store.find(key);
    if(it != kv_store.end()){
      if(!it->second.has_expiry || std::chrono::steady_clock::now() <= it->second.expiry_time){
        type_result = "string";
      }
    }
  }

  if(type_result == "none"){
    {
      std::shared_lock lock(list_mutex);
      auto it = list_store.find(key);
      if(it != list_store.end()){
        type_result = "list";
      }
    }
  }

  if(type_result == "none"){
    {
      std::shared_lock lock(stream_mutex);
      auto it = stream_store.find(key);
      if(it != stream_store.end()){
        type_result = "stream";
      }
    }
  }

  send_response(fd, resp_simple(type_result));
}

void handle_xadd(int fd, const std::vector<std::string>& args){
  if(args.size() < 4 || ((args.size()-3)&1) ){
    send_response(fd, resp_error("wrong number of arguments for 'xadd'"));
    return;
  }

  const std::string& stream_key = args[1];
  const std::string& entry_ID = args[2];

  //FULL AUTO GENERATION
  if(entry_ID == "*"){
    uint64_t ms = static_cast<uint64_t>( std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
    uint64_t seq = 0;
    StreamID generated_id;
    generated_id.ms = ms, generated_id.seq = seq;
   
   { 
      std::unique_lock lock(stream_mutex);
      auto it = stream_store.find(stream_key);
      bool has_entries = it != stream_store.end() && !it->second.empty();
      if(has_entries){
        //get the top element
        const StreamID& last_entry_key = (it -> second).rbegin() -> first;
        uint64_t last_ms = last_entry_key.ms;
        uint64_t last_seq = last_entry_key.seq;

        seq = (ms == last_ms) ? last_seq + 1 : 0;
          
      }

      generated_id.seq = seq;
      std::vector<std::pair<std::string, std::string>>& v = stream_store[stream_key][generated_id];
      for(size_t i = 3 ; i < args.size() ; i+=2){
        const std::string& key = args[i], value = args[i+1];
        v.push_back({key, value});
      }
  }

    touch_key(stream_key);
    if(g_role == "master") propagate_to_replicas(resp_array(args));
    send_response(fd, resp_bulk(generated_id.to_string()));
    return;
  }

  //we have entry id it should be ms-seq
  size_t dash = entry_ID.find('-');
  if(dash == std::string::npos || dash == 0 || dash+1 >= entry_ID.size()){
    send_response(fd, resp_error("Invalid stream ID specified as stream command argument"));
    return;
  }

  std::string str_ms = entry_ID.substr(0, dash);
  std::string str_seq = entry_ID.substr(dash+1);

  bool is_partial_auto = !str_ms.empty() && std::all_of(str_ms.begin(), str_ms.end(), ::isdigit) && (str_seq == "*");

  bool is_explicit_id = !str_ms.empty() && std::all_of(str_ms.begin(), str_ms.end(), ::isdigit) && !str_seq.empty() && std::all_of(str_seq.begin(), str_seq.end(), ::isdigit);

  //PARTIAL AUTO GENERATION
  if(is_partial_auto){
    auto parsed_ms = safe_parse<uint64_t>(str_ms);
    if(!parsed_ms){
      send_response(fd, resp_error("Invalid stream ID specified as stream command argument"));
      return;
    }
    uint64_t ms = *parsed_ms;
    uint64_t seq = 0;
    std::string response;
    StreamID generated_id;
    bool inserted = false;
    {
      std::unique_lock lock(stream_mutex);
      auto it = stream_store.find(stream_key);
      bool has_entries = it != stream_store.end() && !it->second.empty();
      if(has_entries){
        //get the top element
        const StreamID& last_entry_key = (it -> second).rbegin() -> first;
       
        uint64_t last_ms = last_entry_key.ms;
        uint64_t last_seq = last_entry_key.seq;

        if(ms < last_ms){
          response = resp_error("The ID specified in XADD is equal or smaller than the target stream top item");
        } else if(ms == last_ms) seq = last_seq + 1 ;
        else seq = (ms == 0) ? 1 : 0;
          
      } else{
        seq = (ms == 0);
      }

      if(response.empty()){
        generated_id.ms = ms, generated_id.seq = seq;

        std::vector<std::pair<std::string, std::string>>& v = stream_store[stream_key][generated_id];
        for(size_t i = 3 ; i < args.size() ; i+=2){
          const std::string& key = args[i], value = args[i+1];
          v.push_back({key, value});
        }
        response = resp_bulk(generated_id.to_string());
        inserted = true;
      }

    }

    touch_key(stream_key);
    if(inserted && g_role == "master") propagate_to_replicas(resp_array(args));
    send_response(fd, response);
    return;
  }
  
  //EXPLICIT ID
  if(is_explicit_id){
    auto parsed_ms = safe_parse<uint64_t>(entry_ID.substr(0, dash));
    auto parsed_seq = safe_parse<uint64_t>(entry_ID.substr(dash+1));
    if(!parsed_ms || !parsed_seq){
      send_response(fd, resp_error("Invalid stream ID specified as stream command argument"));
      return;
    }
    uint64_t ms = *parsed_ms;
    uint64_t seq = *parsed_seq;

    StreamID generated_id{ms, seq};
   
    if(ms == 0 && seq == 0){
      send_response(fd, resp_error("The ID specified in XADD must be greater than 0-0"));
      return;
    }

    std::string response;

    uint64_t last_ms = 0, last_seq = 0;
    {
      std::unique_lock lock(stream_mutex);
      auto it = stream_store.find(stream_key);
      bool has_entries = it != stream_store.end() && !it->second.empty();
      if(has_entries){
        const StreamID& last_entry_key = (it -> second).rbegin() -> first;
        last_ms = last_entry_key.ms;
        last_seq = last_entry_key.seq;
      }

      bool valid = (ms > last_ms) || (ms == last_ms && seq > last_seq);

      if(valid){

        std::vector<std::pair<std::string, std::string>>& v = stream_store[stream_key][generated_id];
        for(size_t i = 3 ; i < args.size() ; i+=2){
          const std::string& key = args[i], value = args[i+1];
          v.push_back({key, value});
        }
        response = resp_bulk(generated_id.to_string());

      } else response = resp_error("The ID specified in XADD is equal or smaller than the target stream top item");
        
    }

    touch_key(stream_key);
    send_response(fd, response);
    return;
  }

  send_response(fd, resp_error("Invalid stream ID specified as stream command argument"));
  
}

void handle_xrange(int fd, const std::vector<std::string>& args){
  if(args.size() < 4){
    send_response(fd, resp_error("wrong number of arguments for 'xrange'"));
    return;
  }

  const std::string& stream_key = args[1];
  const std::string& start_str = args[2];
  const std::string& end_str = args[3];

  StreamID start_id, end_id;
  if(!StreamID::parse_explicit_stream_id(start_str, start_id, 0) || !StreamID::parse_explicit_stream_id(end_str, end_id, 1)){
    send_response(fd, resp_error("Invalid stream ID specified as stream command argument"));
    return;
  }

  std::string response;

  {
    std::shared_lock lock(stream_mutex);
    auto it = stream_store.find(stream_key);
    if(it == stream_store.end() || it->second.empty()){
      response = "*0\r\n"; //empty array
    } else{
      const StreamLog& stream = it->second;
      std::vector<std::string> matching_entries;

      for(auto entry_it = stream.lower_bound(start_id); entry_it != stream.end() && entry_it->first <= end_id; entry_it++){
        const StreamID& cur_id = entry_it->first;
        const StreamFields& fields = entry_it->second;
        std::string fields_resp = "*" + std::to_string(fields.size()*2)+"\r\n";
        for(const auto& [k, v] : fields){
          fields_resp += resp_bulk(k);
          fields_resp += resp_bulk(v);
        }

        std::string entry_resp = "*2\r\n" + resp_bulk(cur_id.to_string()) + fields_resp;
        matching_entries.push_back(entry_resp);
      }

      response = "*" + std::to_string(matching_entries.size()) + "\r\n";
      for(const auto& entry : matching_entries) response += entry;
    }
  }

  send_response(fd, response);

}

void handle_xread(int fd, const std::vector<std::string>& args){
  if(args.size() < 4 || (args.size()&1)){
    send_response(fd, resp_error("wrong number of arguments for 'xread'"));
    return;
  }

  std::string streams_cmd = args[1];
  for(auto& ch : streams_cmd) ch = std::tolower(ch);
  if(streams_cmd != "streams"){
    send_response(fd, resp_error("syntax error"));
    return;
  }

  // (sz-2)/2 + 2 => sz/2 + 1
  size_t sz = args.size();
  std::vector<StreamID> start_ids;
  for(size_t i = sz/2+1 ; i < args.size() ; i++){
    StreamID parsed_id;
    if(!StreamID::parse_explicit_stream_id(args[i], parsed_id, 0)){
      send_response(fd, resp_error("Invalid stream ID specified as stream command argument"));
      return;
    }
    start_ids.push_back(parsed_id);
  }

  std::string response;

  //now all ids are valid and parsed
  {
    std::shared_lock lock(stream_mutex);
    std::vector<std::string> stream_blocks;
    for(size_t i = 2, j = sz/2+1 ; j < args.size() ; i++, j++){
      const std::string& stream_key = args[i];
      const StreamID& start_id = start_ids[j - (sz/2+1)];
      auto it = stream_store.find(stream_key);
      if(it == stream_store.end() || it->second.empty()){
        continue;
      }

      const StreamLog& stream = it->second;
      std::vector<std::string> matching_entries;
      for(auto entry_it = stream.upper_bound(start_id) ; entry_it != stream.end() ; entry_it++){
        const StreamID& cur_id = entry_it -> first;
        const StreamFields& fields = entry_it -> second;

        std::string resp_fields = "*"+std::to_string(fields.size()*2)+"\r\n";
        for(const auto& [k, v]: fields){
          resp_fields += resp_bulk(k);
          resp_fields += resp_bulk(v);
        }

        std::string resp_log = "*2\r\n" + resp_bulk(cur_id.to_string()) + resp_fields;

        matching_entries.push_back(resp_log);
      }

      if(!matching_entries.empty()){
        std::string inner_entries = "*2\r\n"+resp_bulk(stream_key) + "*"+std::to_string(matching_entries.size()) + "\r\n";
        for(auto& entry_str : matching_entries) inner_entries += entry_str;
        stream_blocks.push_back(inner_entries);
      }

    }

    if(!stream_blocks.empty()){
      response = "*"+std::to_string(stream_blocks.size())+"\r\n";
      for(auto& block : stream_blocks) response += block;
    } else{
      response = "*-1\r\n";
    }

  }

  send_response(fd, response);
}

void handle_incr(int fd, const std::vector<std::string>& args){
  if(args.size() < 2){
    send_response(fd, resp_error("wrong number of arguments for 'incr'"));
    return;
  }

  const std::string& key = args[1];
  std::string response;
  bool is_incr = true;
  {
    std::unique_lock lock(kv_mutex);
    auto it = kv_store.find(key);
    if(it == kv_store.end()){
      Value val;
      val.value = "1";
      kv_store.insert({key, val});
      response = resp_integer(1);
    } else{
      Value& val = kv_store[key];
      const std::string val_str = val.value;
      bool is_integer = std::all_of(val_str.begin(), val_str.end(), ::isdigit);

      if(is_integer){
        auto parsed = safe_parse<long long>(val_str);
        if(!parsed){
          is_incr = false;
          response = resp_error("value is not an integer or out of range");
        } else if(*parsed == LLONG_MAX){
          is_incr = false;
          response = resp_error("increment or decrement would overflow");
        } else{
          long long next_val = *parsed + 1;
          val.value = std::to_string(next_val);
          response = resp_integer(next_val);
        }
      } else{
        is_incr = false;
        response = resp_error("value is not an integer or out of range");
      }
    }
  }
  if(is_incr){
    touch_key(key);
    if(g_role == "master") propagate_to_replicas(resp_array(args));
  } 
  send_response(fd, response);
}

void handle_multi(int fd, ClientState& state){
  if(state.in_transaction){
    send_response(fd, resp_error("MULTI calls can not be nested"));
    return;
  }
  state.in_transaction = true;
  state.transaction_queue.clear();
  send_response(fd, resp_ok());
}

void handle_discard(int fd, ClientState& state){
  if(!state.in_transaction){
    send_response(fd, resp_error("DISCARD without MULTI"));
    return;
  }

  state.in_transaction = false;
  state.transaction_queue.clear();
  unwatch_all(state);
  send_response(fd, resp_ok());
}

void handle_exec(int fd, ClientState& state){
  if(!state.in_transaction){
    send_response(fd, resp_error("EXEC without MULTI"));
    return;
  }

  state.in_transaction = false;

  if(state.watched_keys_modified){
    //watched keys modifed -> abort
    state.transaction_queue.clear();
    unwatch_all(state);
    send_response(fd, resp_null());
    return;
  }

  unwatch_all(state);

  if(state.transaction_queue.empty()){
    send_response(fd, "*0\r\n");
    return;
  }

  auto queue = std::move(state.transaction_queue);
  state.transaction_queue.clear();

  std::vector<std::string> collected_responses;

  for(const auto& cmd_args : queue){
    std::string single_output;

    //trap void handler's output
    g_reponse_trap = &single_output;
    dispatch_command(fd, cmd_args);
    g_reponse_trap = nullptr;

    collected_responses.push_back(single_output);
  }

  std::string exec_response = "*"+std::to_string(collected_responses.size())+"\r\n";
  for(const auto& res : collected_responses){
    exec_response += res;
  }

  unwatch_all(state);
  send_response(fd, exec_response);  
}

//this is called whenever a key is modified
void touch_key(const std::string& key){
  std::lock_guard lock(g_watch_mutex);
  auto it = g_watched_keys_registry.find(key);
  if(it != g_watched_keys_registry.end()){
    for(ClientState*client : it -> second){
      client -> watched_keys_modified = true;
    }
  }
}

void unwatch_all(ClientState& state){
  std::lock_guard lock(g_watch_mutex);
  for(const std::string& key : state.watched_keys){
    auto it = g_watched_keys_registry.find(key);
    if(it != g_watched_keys_registry.end()){
      it->second.erase(&state);
      if(it->second.empty()){
        g_watched_keys_registry.erase(it);
      }
    }
  }
  state.watched_keys.clear();
  state.watched_keys_modified = false;
}

void handle_watch(int fd, const std::vector<std::string>& args, ClientState& state){
  if(args.size() < 2){
    send_response(fd, resp_error("wrong number of arguments for 'watch'"));
    return;
  }

  if(state.in_transaction){
    send_response(fd, resp_error("WATCH inside MULTI is not allowed"));
    return;
  }

  {
    std::lock_guard lock(g_watch_mutex);
    for(size_t i = 1 ; i < args.size() ; i++){
      const std::string& key = args[i];
      g_watched_keys_registry[key].insert(&state);
      state.watched_keys.insert(key);
    }
  }

  send_response(fd, resp_ok());
}

void handle_unwatch(int fd, ClientState& state){
  if(state.in_transaction){
    send_response(fd, resp_error("UNWATCH inside MULTI is not allowed"));
    return;
  }
  unwatch_all(state);
  send_response(fd, resp_ok());
}

void handle_info(int fd, const std::vector<std::string>& args){
  std::string section = "default";
  if(args.size() > 1){
    section = args[1];
    for(auto& ch : section) ch = std::tolower(ch);
  }

  std::string info_text = "";

  if(section == "replication" || section == "default"){
    info_text += "# Replication\r\n";
    info_text += "role:" + g_role + "\r\n";
    if(g_role == "master"){
      info_text += "master_replid:" + g_master_replid + "\r\n";
      info_text += "master_repl_offeset:"+std::to_string(g_master_repl_offset)+"\r\n";

      size_t num_slaves = 0;
      {
        std::unique_lock lock(g_replicas_mutex);
        num_slaves = g_replicas.size();
      }
      info_text += "connected_slaves:" + std::to_string(num_slaves) + "\r\n";
    }
  }

  send_response(fd, resp_bulk(info_text));
}

void handle_replconf(int fd, const std::vector<std::string>& args){
  if(args.size() >= 3){
    std::string sub = args[1];
    for(auto& ch : sub) ch = std::tolower(ch);

    if(sub == "ack"){
      auto parsed = safe_parse<long long>(args[2]);
      long long ack_offset = parsed ? *parsed : 0;

      {
        std::unique_lock lock(g_replica_offsets_mutex);
        g_replica_offsets[fd] = ack_offset;
      }

      g_replica_cv.notify_all();
    }
  }
  send_response(fd, resp_ok());
}

void handle_psync(int fd, [[maybe_unused]] const std::vector<std::string>& args){
  //+FULLRESYNC <repid> 0
  std::string fullresync_msg = "+FULLRESYNC " + g_master_replid + " 0\r\n";
  send_response(fd, fullresync_msg);

  //send RDB binary snapshot
  std::string rdb_binary = hex_to_bytes(EMPTY_RDB_HEX);
  std::string rdb_header = "$" + std::to_string(rdb_binary.size()) + "\r\n";

  send(fd, rdb_header.data(), rdb_header.size(), 0);
  send(fd, rdb_binary.data(), rdb_binary.size(), 0);

  //register this socket (replica)
  {
    std::unique_lock lock(g_replicas_mutex);
    g_replicas.push_back(fd);
  }
}

void handle_wait(int fd, const std::vector<std::string>& args){
  if(args.size() < 3){
    send_response(fd, resp_error("wrong number of arguments for 'wait'"));
    return;
  }

  auto parsed_replicas = safe_parse<int>(args[1]);
  auto parsed_timeout = safe_parse<long long>(args[2]);
  if(!parsed_replicas || !parsed_timeout){
    send_response(fd, resp_error("invalid arguments for 'wait'"));
    return;
  }
  int expected_replicas = *parsed_replicas;
  long long timeout_ms = *parsed_timeout;

  //snapshot current offset under replicas lock to avoid data race
  long long current_offset;
  {
    std::unique_lock lock(g_replicas_mutex);
    current_offset = g_master_repl_offset;
  }

  //if no write commands have been propagated, return replica count
  if(current_offset == 0){
    std::unique_lock lock(g_replicas_mutex);
    send_response(fd, resp_integer(g_replicas.size()));
    return;
  }

  // broadcast replconf getack to all connected replicas
  std::string getack_cmd = resp_array({"REPLCONF", "GETACK", "*"});
  {
    std::unique_lock lock(g_replicas_mutex);
    for(int rep_fd : g_replicas){
      send(rep_fd, getack_cmd.data(), getack_cmd.size(), 0);
    }
  }

  //count helper
  auto count_acked = [&](){
    int count = 0;
    for(const auto& [rep_fd, offset] : g_replica_offsets){
      if(offset >= current_offset){
        count++;
      }
    }
    return count;
  };

  //block until count is satisfied or timeout expires
  auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  int acked_count = 0;

  {
    std::unique_lock lock(g_replica_offsets_mutex);
    while(true){
      acked_count = count_acked();
      if(acked_count >= expected_replicas) break;

      if(timeout_ms == 0){
        g_replica_cv.wait(lock);
      } else{
        if(g_replica_cv.wait_until(lock, deadline) == std::cv_status::timeout){
          acked_count = count_acked();
          break;
        }
      }
    }
  }

  send_response(fd, resp_integer(acked_count));

}

void propagate_to_replicas(const std::string& raw_resp_cmd){
  std::unique_lock lock(g_replicas_mutex);
  for(int replica_fd : g_replicas){
    send(replica_fd, raw_resp_cmd.c_str(), raw_resp_cmd.size(), 0);
  }
  g_master_repl_offset += raw_resp_cmd.size();
}

void handle_config(int fd, const std::vector<std::string>& args){
  if(args.size() < 3){
    send_response(fd, resp_error("wrong number of arguments for 'config"));
    return;
  }

  std::string sub = args[1];
  for(auto& ch : sub) ch = std::tolower(ch);

  if(sub == "get"){
    std::string param = args[2];
    for(auto& ch : param) ch = std::tolower(ch);

    if(param == "dir"){
      send_response(fd, resp_array({"dir", g_rdb_dir}));
      return;
    } else if(param == "dbfilename"){
      send_response(fd, resp_array({"dbfilename", g_rdb_filename}));
      return;
    } else{
      send_response(fd, resp_array({}));
      return;
    }
  }
  send_response(fd, resp_error("unknown subcommand for 'config'"));
}

void handle_keys(int fd, const std::vector<std::string>& args){
  if(args.size() < 2){
    send_response(fd, resp_error("wrong number of arguments for 'keys'"));
    return;
  }

  std::string pattern = args[1];
  std::vector<std::string> matched_keys;
  {
    std::shared_lock lock(kv_mutex);
    for(const auto& [k, v] : kv_store){
      if(v.has_expiry && std::chrono::steady_clock::now() > v.expiry_time){
        continue;
      }
      if(pattern == "*"){
        matched_keys.push_back(k);
      }
    }
  }

  send_response(fd, resp_array(matched_keys));
}

void handle_command(int fd, [[maybe_unused]] const std::vector<std::string>& args){
  send_response(fd, "*0\r\n");
}

void dispatch_command(int fd, const std::vector<std::string>& args){
  if(args.empty()) return;

  std::string cmd = args[0];
  for(auto& ch : cmd) ch = std::tolower(ch);

  auto it = handlers.find(cmd);

  if(it == handlers.end()){
    send_response(fd, resp_error("unknown command '" + cmd + "'"));
  } else{
    it->second(fd, args);
  }
}

bool parse_single_resp_command(std::string_view sv, std::vector<std::string>& out_args, int& consumed_bytes){
  consumed_bytes = 0;
  if(sv.empty()) return false;

  //handle inline commands (plain ascii ending in \r\n used by redis-benchmark PING_INLINE)

  if(sv[0] != '*'){
    size_t pos = sv.find("\r\n");
    if(pos == std::string_view::npos) return false;

    std::string_view line = sv.substr(0, pos);
    consumed_bytes = pos+2;

    //tokenize line by whitespace
    std::vector<std::string> args;
    size_t start = 0;
    while(start < line.size()){
      while(start < line.size() && std::isspace(static_cast<unsigned char>(line[start]))) start++;

      if(start >= line.size()) break;

      //find end of token
      size_t end = start;
      while(end < line.size() && !std::isspace(static_cast<unsigned char>(line[end]))) end++;

      args.emplace_back(line.substr(start, end-start));
      start = end;
    }

    if(args.empty()) return false;
    out_args = std::move(args);
    return true;
  }

  size_t cursor = 1;

  auto read_line = [&](std::string_view& line) -> bool{
    size_t pos = sv.find("\r\n", cursor);
    if(pos == std::string_view::npos) return false;
    line = sv.substr(cursor, pos-cursor);
    cursor = pos+2;
    return true;
  };

  std::string_view header;
  if(!read_line(header)) return false;

  int num_args = 0;
  auto [p1, ec1] = std::from_chars(header.data(), header.data() + header.size(), num_args);

  if(ec1 != std::errc{} || num_args <= 0) return false;

  std::vector<std::string> args;
  args.reserve(num_args);

  for(int i = 0 ; i < num_args ; i++){
    std::string_view len_line;
    if(!read_line(len_line) || len_line.empty() || len_line[0] != '$') return false;

    int str_len = 0;
    auto [p2, ec2] = std::from_chars(len_line.data() + 1, len_line.data() + len_line.size(), str_len);
    if(ec2 != std::errc{} || str_len < 0) return false;

    if(cursor + str_len + 2 > sv.size()) return false; // incomplete payload
    std::string_view arg_val = sv.substr(cursor, str_len);
    if(sv.substr(cursor+str_len, 2) != "\r\n") return false;

    args.emplace_back(arg_val);
    cursor += str_len + 2;
  }

  consumed_bytes += cursor;
  out_args = std::move(args);
  return true;
}

void RESP_Parser(int client_fd, std::vector<std::string>& args, ClientState& state){
  
  if(args.empty()) return;

  std::string command = args[0];
  for(auto& ch : command) ch = std::tolower(ch);

  //Connection-level transaction Handlers
  if(command == "multi"){
    handle_multi(client_fd, state);
    return;
  }

  if(command == "discard"){
    handle_discard(client_fd, state);
    return;
  }

  if(command == "exec"){
    handle_exec(client_fd, state);
    return;
  }

  if(command == "watch"){
    handle_watch(client_fd, args, state);
    return;
  }

  if(command == "unwatch"){
    handle_unwatch(client_fd, state);
    return;
  }

  // Transaction Intercept Gate
  if(state.in_transaction){
    state.transaction_queue.push_back(args);
    send_response(client_fd, resp_simple("QUEUED"));
    return;
  }

  dispatch_command(client_fd, args);
}

void handle_client(int client_fd){
  char buffer[4096];
  std::string accumulator;

  ClientState state;
  while(true){
    memset(buffer, 0, sizeof(buffer));
    int bytes_received = recv(client_fd, buffer, sizeof(buffer), 0);
    if(bytes_received <= 0) break;

    accumulator.append(buffer, bytes_received);

    //Drain all fully received commands from accumulator
    while(!accumulator.empty()){
      std::vector<std::string> arguments;
      int consumed_bytes = 0;

      if(!parse_single_resp_command(accumulator, arguments, consumed_bytes)){
        //incomplete frame
        break;
      }

      RESP_Parser(client_fd, arguments, state);

      accumulator.erase(0, consumed_bytes);
    }
  }
  close(client_fd);
}

void replica_replication_loop(int master_fd){
  char buffer[4096];
  std::string accumulator;

  long long replica_offset = 0;
  ClientState dummy_state;
  
  while(true){
    memset(buffer, 0, sizeof(buffer));
    int bytes_received = recv(master_fd, buffer, sizeof(buffer), 0);
    if(bytes_received <= 0) break; // master disconnected

    accumulator.append(buffer, bytes_received);

    //Drain all fully received commands from accumulator
    while(!accumulator.empty()){
      std::vector<std::string> args;
      int consumed_bytes = 0;

      if(!parse_single_resp_command(accumulator, args, consumed_bytes)){
        //incomplete frame
        break;
      }

      if(!args.empty()){
        std::string cmd = args[0];
        for(auto& ch : cmd) ch = std::tolower(ch);

        if(cmd == "replconf" && args.size() >= 2){
          std::string sub = args[1];
          for(auto& ch : sub) ch = std::tolower(ch);
          if(sub == "getack"){
            std::string ack_reply = resp_array({"REPLCONF", "ACK", std::to_string(replica_offset)});
            send(master_fd, ack_reply.data(), ack_reply.size(), 0);
          }
        } else{
          RESP_Parser(-1, args, dummy_state);
        }
      }

      replica_offset += consumed_bytes;
      accumulator.erase(0, consumed_bytes);
    }
  }
  close(master_fd);
}

bool send_and_expect(int fd, const std::string& cmd){
  if(send(fd, cmd.data(), cmd.size(), 0) < 0) return false;
  char buf[1024];
  memset(buf, 0, sizeof(buf));
  int bytes = recv(fd, buf, sizeof(buf), 0);
  return bytes > 0;
}

void initiate_replica_handshake(int my_port){
  int master_fd = socket(AF_INET, SOCK_STREAM, 0);
  if(master_fd < 0){
    std::cerr << "Failed to create socket to master\n";
    return;
  }

  struct sockaddr_in master_addr;
  master_addr.sin_family = AF_INET;
  master_addr.sin_port = htons(g_master_port);

  if(g_master_host == "localhost"){
    inet_pton(AF_INET, "127.0.0.1", &master_addr.sin_addr);
  } else{
    inet_pton(AF_INET, g_master_host.c_str(), &master_addr.sin_addr);
  }

  if(connect(master_fd, (struct sockaddr*)&master_addr, sizeof(master_addr)) < 0){
    std::cerr << "Failed to connect to master at "<<g_master_host<<":"<<g_master_port<<"\n";
    close(master_fd);
    return;
  }

  //handshake state1 : ping -> pong
  if(!send_and_expect(master_fd, resp_array({"PING"}))){
    std::cerr << "Handshake Step 1 (PING) failed\n";
    close(master_fd);
    return;
  }

  
  if(!send_and_expect(master_fd, resp_array({"REPLCONF", "listening-port", std::to_string(my_port)}))){
    std::cerr << "Handshake Step 2a (listening-port) failed\n";
    close(master_fd);
    return;
  }

  if(!send_and_expect(master_fd, resp_array({"REPLCONF", "capa", "psync2"}))){
    std::cerr << "Handshake Step 2b (capa psync2) failed\n";
    close(master_fd);
    return;
  }

  std::string psync_cmd = resp_array({"PSYNC", "?", "-1"});
  if(send(master_fd, psync_cmd.data(), psync_cmd.size(), 0) < 0){
    std::cerr << "Handshake Step 3 (PSYNC) failed\n";
    close(master_fd);
    return;
  }

  //read and discard initial FULLRESYNC and RDB file snapshot
  char buf[4096];
  std::string sync_buf;

  while(true){
    int bytes = recv(master_fd, buf, sizeof(buf), 0);
    if(bytes <= 0){
      std::cerr << "Failed to receive master sync response\n";
      close(master_fd);
      return;
    }
    sync_buf.append(buf, bytes);

    //find end of +FULLRESYNC line
    auto line_end = sync_buf.find("\r\n");
    if(line_end == std::string::npos) continue;

    //find start of RDB payload: $<len>\r\n
    std::string after_line = sync_buf.substr(line_end + 2);
    auto rdb_hdr_end = after_line.find("\r\n");
    if(rdb_hdr_end == std::string::npos) continue;

    auto parsed_rdb_len = safe_parse<long long>(after_line.substr(1, rdb_hdr_end - 1));
    if(!parsed_rdb_len) continue;
    long long rdb_len = *parsed_rdb_len;

    //total bytes needed after the FULLRESYNC line: $<len>\r\n + rdb_len bytes
    long long total = (line_end + 2) + (rdb_hdr_end + 2) + rdb_len;
    if((long long)sync_buf.size() >= total) break;
  }

  std::cout<<"Handshake completed\n";

  //handshake completed -> hand master_fd to continuous replication processing
  replica_replication_loop(master_fd);

}

void load_rdb_file(){
  if(g_rdb_dir.empty() || g_rdb_filename.empty()){
    return;
  }

  std::string full_path = g_rdb_dir + "/" + g_rdb_filename;

  RDBReader reader(full_path);

  if(!reader.is_open()){
    std::cout << "No RDB file found at " << full_path << ". Starting with empty storage.\n";
    return;
  }

  char header[9];
  if(!reader.read_exact(header, 9)) return;
  std::string magic(header, 5);
  if(magic != "REDIS"){
    std::cerr << "Invalid RDB magic header!\n";
    return;
  }

  uint8_t opcode;
  while(reader.read_byte(opcode)){
    //0xFF: End of file 
    if(opcode == 0xFF){
      break;
    }

    //Auxiliary field
    if(opcode == 0xFA){
      std::string aux_key, aux_val;
      reader.read_string(aux_key);
      reader.read_string(aux_val);
      continue;
    }

    //DB selector
    if(opcode == 0xFE){
      uint32_t db_num = 0;
      bool is_special = false;
      reader.read_length(db_num, is_special);
      continue;
    }

    //Resizedb
    if(opcode == 0xFB){
      uint32_t db_size = 0, expiry_size = 0;
      bool is_special = false;
      reader.read_length(db_size, is_special);
      reader.read_length(expiry_size, is_special);
      continue;
    }

    //reading keys, values, and expiration
    bool has_expiry = false;
    std::chrono::time_point<std::chrono::steady_clock> expiry_time;

    //expiry timestamp in ms
    if(opcode == 0xFC){
      uint64_t ms = 0;
      reader.read_exact(reinterpret_cast<char*>(&ms), 8);
      has_expiry = true;

      auto now_sys = std::chrono::system_clock::now();
      auto target_sys = std::chrono::system_clock::time_point(std::chrono::milliseconds(ms));
      auto duration = target_sys - now_sys;
      expiry_time = std::chrono::steady_clock::now() + std::chrono::duration_cast<std::chrono::milliseconds>(duration);

      reader.read_byte(opcode);
    } else if(opcode == 0xFD){
      uint64_t sec = 0;
      reader.read_exact(reinterpret_cast<char*>(&sec), 4);
      has_expiry = true;

      auto now_sys = std::chrono::system_clock::now();
      auto target_sys = std::chrono::system_clock::time_point(std::chrono::seconds(sec));
      auto duration = target_sys - now_sys;
      expiry_time = std::chrono::steady_clock::now() + std::chrono::duration_cast<std::chrono::milliseconds>(duration);

      reader.read_byte(opcode);
    }

    //opcode is 0x00 -> string
    uint8_t val_type = opcode;

    std::string key;
    if(!reader.read_string(key)) break;

    if(val_type == 0x00){
      std::string val;
      if(!reader.read_string(val)) break;

      if(has_expiry && std::chrono::steady_clock::now() > expiry_time){
        continue;
      }

      Value entry;
      entry.value = val;
      entry.has_expiry = has_expiry;
      entry.expiry_time = expiry_time;

      {
        std::unique_lock lock(kv_mutex);
        kv_store[key] = entry;
      }
    }
  }

  std::cout << "RDB file loaded successfully into kv_store.\n";
}

int main(int argc, char **argv) {
  // Flush after every std::cout / std::cerr
  int PORT = 6379;
  
  for(int i = 1 ; i < argc ; i++){
    std::string arg = argv[i];
    if(arg == "--port" && i + 1 < argc){
      auto parsed = safe_parse<int>(argv[i+1]);
      if(!parsed){
        std::cerr << "Invalid port\n";
        return 1;
      }
      PORT = *parsed;
    } else if(arg == "--replicaof" && i+2 < argc){
      g_role = "slave";
      g_master_host = argv[i+1];
      auto parsed = safe_parse<int>(argv[i+2]);
      if(!parsed){
        std::cerr << "Invalid master port\n";
        return 1;
      }
      g_master_port = *parsed;
    } else if(arg == "--dir" && i+1 < argc){
      g_rdb_dir = argv[i+1];
    } else if(arg == "--dbfilename" && i+1 < argc){
      g_rdb_filename = argv[i+1];
    }
    i++;
  }

  load_rdb_file();

  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;
  
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
   std::cerr << "Failed to create server socket\n";
   return 1;
  }
  
  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }
  
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(PORT);
  
  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port " << PORT <<"\n";
    return 1;
  }
  
  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }
  
  std::cout<< "Redis server listening on port "<< PORT << "...\n";

  //Replica Handshake Thread
  if(g_role == "slave"){
    std::thread(initiate_replica_handshake, PORT).detach();
  }
  
  // You can use print statements as follows for debugging, they'll be visible when running tests.
  std::cout << "Logs from your program will appear here!\n";
  std::cout << "Waiting for a client to connect...\n";

  //multiple clients
  while(true){
    struct sockaddr_in client_addr;
    int client_addr_len = sizeof(client_addr);
    int client_fd = accept(server_fd, (struct sockaddr *) &client_addr, (socklen_t *) &client_addr_len);
    if(client_fd < 0) continue;

    std::thread(handle_client, client_fd).detach();
  }
  
  close(server_fd);

  return 0;
}
