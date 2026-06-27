#include <atomic>
#include <condition_variable>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <system_error>
#include <thread>

#define SYS_ERR -1
#define OK 0
#define DEBUG(str) std::cerr << "[DEBUG] " << str;

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
						task_available_.wait(task_q_lock, [this]() {
							return kill_all_threads_ || !task_q_.empty();
						});

						if (kill_all_threads_) {
							active_thread_cnt_.fetch_sub(1);
							alive_threads_.notify_all();
							return;
						} else {
							task = std::move(task_q_.front());
							task_q_.pop();
							task_q_lock.unlock();

							working_threads_cnt_.fetch_add(1);

							task();

							working_threads_cnt_.fetch_sub(1);
							ongoing_work_.notify_all();
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

		task_available_.notify_one();
	}

	static inline std::shared_ptr<BlockingThreadPool>
	makeSharedPtrTo(int n = 0) {
		auto obj = std::make_shared<BlockingThreadPool>();
		if (obj->trySpawnThreads(n) != OK)
			return nullptr;
		return obj;
	}

	inline void forceClearQueue() {
		std::lock_guard<std::mutex> take(task_q_mtx_);
		while (!task_q_.empty())
			task_q_.pop();
		// DEBUG("forceClearQueue(): queue emptied\n")
	}

	inline void waitForTasksComplete() {
		std::unique_lock<std::mutex> take(task_q_mtx_);
		ongoing_work_.wait(take, [this]() {
			// DEBUG("waitForTasksComplete(): checking. working_threads_cnt_ = " << working_threads_cnt_
			// 																  << " queue size: " << task_q_.size() << '\n');
			return working_threads_cnt_ == 0 && task_q_.size() == 0;
		});

		// DEBUG("waitForTasksComplete(): queue emptied and working threads = 0\n")
	}

	inline int waitForTasksCompleteAndHarvestThreads() {
		try {
			std::unique_lock<std::mutex> task_q_lock(task_q_mtx_);

			// DEBUG("waitForTasksCompleteAndHarvestThreads(): waiting for working_thread_cnt_ and task_q.size()\n");
			ongoing_work_.wait(task_q_lock, [this]() {
				return working_threads_cnt_ == 0 && task_q_.size() == 0;
			});

			// DEBUG("waitForTasksCompleteAndHarvestThreads(): working threads = 0, task q empty.\n");

			kill_all_threads_.store(true);
			task_available_.notify_all();

			alive_threads_.wait(task_q_lock, [this] { return active_thread_cnt_ == 0; });

			// DEBUG("waitForTasksCompleteAndHarvestThreads(): active_thread_cnt = 0\n");

			for (auto &worker : threads_vec_) {
				if (worker.joinable()) {
					worker.join();
					joinable_thread_cnt_.fetch_sub(1);
				}
			}
			threads_vec_.clear();
			// DEBUG("forceterminateThreads(): threads harvested\n");

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
			task_available_.notify_all();

			alive_threads_.wait(task_q_lock, [this] { return active_thread_cnt_ == 0; });
			// DEBUG("forceterminateThreads(): wait completed, " << active_thread_cnt_ << " active threads.\n must join " << threads_vec_.size() << " threads.\n");

			for (auto &worker : threads_vec_) {
				if (worker.joinable()) {
					// DEBUG("forceterminateThreads(): " << "joining a thread.\n");
					worker.join();
					joinable_thread_cnt_.fetch_sub(1);
					// DEBUG("forceterminateThreads(): " << joinable_thread_cnt_ << " threads remaining to join.\n");
				} // else DEBUG("forceterminateThreads(): " << "encountered an unjoinable thread.\n");
			}
			threads_vec_.clear();
			// DEBUG("forceterminateThreads(): threads harvested\n");

			kill_all_threads_.store(false);
			return OK;
		} catch (std::system_error &e) {
			return SYS_ERR;
		}
	}

	~BlockingThreadPool() {
		// DEBUG("~BlockingThreadPool(): desstructor initiaited\n");
		if (joinable_thread_cnt_ > 0)
			forceTerminateThreads();
		// DEBUG("~BlockingThreadPool(): threads harvested\n");
		if (task_q_.size() > 0)
			forceClearQueue();
		// DEBUG("~BlockingThreadPool(): task queue empty\n");
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
	std::condition_variable task_available_;
	std::condition_variable ongoing_work_;
	std::condition_variable alive_threads_;
	std::queue<std::function<void()>> task_q_;
	std::vector<std::thread> threads_vec_;
	std::atomic<bool> kill_all_threads_ = false;
	std::atomic<int32_t> working_threads_cnt_ = 0;
	std::atomic<int32_t> active_thread_cnt_ = 0;
	std::atomic<int32_t> joinable_thread_cnt_ = 0;
};
} // namespace hhf112
