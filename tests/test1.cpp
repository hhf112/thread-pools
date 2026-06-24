#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "../BlockingThreadPool.hpp"

class PoolTest : public testing::Test {
 protected:
  hhf112::BlockingThreadPool pool;

  void SetUp() override {
    for (int i{10}; i-- > 0;) {
      pool.pushTask(
          [](std::string str) -> void {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            std::cerr << str << '\n';
          },
          "task completed.");
    }
  }

  void TearDown() override {}
};

TEST_F(PoolTest, waitForTasksCompleteAndHarvestThreads) {
  if (pool.trySpawnThreads(5) == -1) return;

  ASSERT_EQ(pool.waitForTasksCompleteAndHarvestThreads(), 0);

  ASSERT_EQ(pool.getActiveThreadsCnt(), 0);
  ASSERT_EQ(pool.getWorkingThreadsCnt(), 0);
  ASSERT_EQ(pool.getTaskQueueSize(), 0);
  ASSERT_EQ(pool.getJoinableThreadsCnt(), 0);
}

TEST_F(PoolTest, waitForTasksComplete) {
  if (pool.trySpawnThreads(5) == -1) return;

  pool.waitForTasksComplete();

  ASSERT_EQ(pool.getActiveThreadsCnt(), 5);
  ASSERT_EQ(pool.getJoinableThreadsCnt(), 5);
  ASSERT_EQ(pool.getWorkingThreadsCnt(), 0);
  ASSERT_EQ(pool.getTaskQueueSize(), 0);
}

TEST_F(PoolTest, forceClearQueue) {
  if (pool.trySpawnThreads(5) == -1) return;

  pool.forceClearQueue();

  ASSERT_EQ(pool.getTaskQueueSize(), 0);
}

// TEST_F(PoolTest, forceTerminateThreads) {
//   if (pool.trySpawnThreads(5) == -1) return;
//
//   ASSERT_EQ(pool.forceTerminateThreads(), OK);
//
//   ASSERT_EQ(pool.getActiveThreadsCnt(), 0);
//   ASSERT_EQ(pool.getJoinableThreadsCnt(), 0);
// }
