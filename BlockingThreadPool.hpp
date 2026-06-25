#include <atomic>
#include <condition_variable>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <system_error>
#include <thread>
#include <iostream>

#define SYS_ERR -1
#define OK 0

namespace hhf112 {
class BlockingThreadPool {
 public:
  BlockingThreadPool() = default;
  BlockingThreadPool(const BlockingThreadPool &workers) = delete;
  BlockingThreadPool &operator=(const BlockingThreadPool &workers) = delete;

  inline int trySpawnThreads(unsigned int threads_cnt = 1) {
    kill_all_threads_ = false;
    try {
      while (threads_cnt-- > 0) {
        threads_vec_.emplace_back(std::thread([this]() -> void {
          active_thread_cnt_.fetch_add(1);
          joinable_thread_cnt_.fetch_add(1);

          std::function<void()> task;

          while (true) {
            std::unique_lock<std::mutex> task_q_lock(task_q_mtx_);
            cv_.wait(task_q_lock, [this]() {
              return kill_all_threads_ || !task_q_.empty();
            });

            if (kill_all_threads_) {
              active_thread_cnt_.fetch_sub(1);
              cv_.notify_all();
              return;
            } else {
              task = std::move(task_q_.front());
              task_q_.pop();
              task_q_lock.unlock();

              working_threads_cnt_.fetch_add(1);

              task();

              working_threads_cnt_.fetch_sub(1);
              cv_.notify_all();
            }
          }
        }));
      }
      return OK;
    } catch (std::system_error &e) {
      return SYS_ERR;
    }
  }

  template <typename Functor, typename... Args>
  inline void pushTask(Functor &&fn, Args &&...args) {
    {
      std::lock_guard<std::mutex> task_q_lock(task_q_mtx_);

      if constexpr (sizeof...(Args) != 0)
        task_q_.push(std::bind_front(std::forward<Functor>(fn),
                                     std::forward<Args>(args)...));
      else
        task_q_.push(fn);
    }

    cv_.notify_one();
  }

  static inline std::shared_ptr<BlockingThreadPool> makeSharedPtrTo(int n = 0) {
    auto obj = std::make_shared<BlockingThreadPool>();
    if (obj->trySpawnThreads(n) != OK) return nullptr;
    return obj;
  }

  inline void forceClearQueue() {
    std::lock_guard<std::mutex> take(task_q_mtx_);
    while (!task_q_.empty()) task_q_.pop();
  }

  inline void waitForTasksComplete() {
    std::unique_lock<std::mutex> take(task_q_mtx_);
    cv_.wait(take, [this]() {
      return working_threads_cnt_ == 0 && task_q_.size() == 0;
    });
  }

  inline int waitForTasksCompleteAndHarvestThreads() {
    try {
      std::unique_lock<std::mutex> task_q_lock(task_q_mtx_);
      cv_.wait(task_q_lock, [this]() {
        return working_threads_cnt_.load() == 0 && task_q_.size() == 0;
      });

      kill_all_threads_.store(true);
      cv_.notify_all();

      cv_.wait(task_q_lock, [this] { return active_thread_cnt_ == 0; });

      for (auto &worker : threads_vec_) {
        if (worker.joinable()) {
          worker.join();
          joinable_thread_cnt_.fetch_sub(1);
        }
      }

      kill_all_threads_.store(false);
      return OK;
    } catch (std::system_error &e) {
      return SYS_ERR;
    }
  }

  inline int forceTerminateThreads() {
    try {
      std::unique_lock<std::mutex> task_q_lock(task_q_mtx_);

      kill_all_threads_.store(true);
      cv_.notify_all();

      cv_.wait(task_q_lock, [this] { return active_thread_cnt_ == 0; });
      std::cerr << "thiswaitcompletes\n";

      for (auto &worker : threads_vec_) {
        if (worker.joinable()) {
          worker.join();
          std::cerr << "thread joined.\n";
          joinable_thread_cnt_.fetch_sub(1);
        }
      }

      kill_all_threads_.store(false);
      std::cerr << "we are done here\n";
      return OK;
    } catch (std::system_error &e) {
      return SYS_ERR;
    }
  }

  ~BlockingThreadPool() {  
    forceTerminateThreads();
    forceClearQueue();
  }

  int32_t getWorkingThreadsCnt() { return working_threads_cnt_.load(); }
  int32_t getActiveThreadsCnt() { return active_thread_cnt_.load(); }
  int32_t getJoinableThreadsCnt() { return joinable_thread_cnt_.load(); }
  std::function<void()> popTaskQueue() {
    std::lock_guard<std::mutex> guard(task_q_mtx_);
    return task_q_.front();
  }
  int32_t getTaskQueueSize() {
    std::lock_guard<std::mutex> take(task_q_mtx_);
    return task_q_.size();
  }

 private:
  std::mutex task_q_mtx_;
  std::condition_variable cv_;
  std::queue<std::function<void()>> task_q_;
  std::vector<std::thread> threads_vec_;
  std::atomic<bool> kill_all_threads_ = false;
  std::atomic<int32_t> working_threads_cnt_ = 0;
  std::atomic<int32_t> active_thread_cnt_ = 0;
  std::atomic<int32_t> joinable_thread_cnt_ = 0;
};
}  // namespace hhf112
