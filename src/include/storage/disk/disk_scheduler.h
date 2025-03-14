//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.h
//
// Identification: src/include/storage/disk/disk_scheduler.h
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <future>  // NOLINT
#include <optional>
#include <thread>  // NOLINT

#include "common/channel.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

/**
 * @brief Represents a Write or Read request for the DiskManager to execute.
 */
struct DiskRequest {
  /** Flag indicating whether the request is a write or a read.
   * 指示请求是写入还是读取的标志。 */
  bool is_write_;

  /**
   *  Pointer to the start of the memory location where a page is either:
   *   1. being read into from disk (on a read).
   *   2. being written out to disk (on a write).
   *   指向内存位置起始处的指针，其中页面处于以下状态：
        1. 从磁盘读入（读取时）。
        2. 写入磁盘（写入时）。
   */
  char *data_;

  /** ID of the page being read from / written to disk.
   *
   * 正在从磁盘读取/写入磁盘的页面的 ID。
   */
  page_id_t page_id_;

  /** Callback used to signal to the request issuer when the request has been completed.
   *
   * 当请求完成时，回调用于向请求发出者发出信号。
   */
  std::promise<bool> callback_;
};

/**
 * @brief The DiskScheduler schedules disk read and write operations.
 *
 * A request is scheduled by calling DiskScheduler::Schedule() with an appropriate DiskRequest object. The scheduler
 * maintains a background worker thread that processes the scheduled requests using the disk manager. The background
 * thread is created in the DiskScheduler constructor and joined in its destructor.
  * DiskScheduler 调度磁盘读写操作。

  通过使用适当的 DiskRequest
  对象调用 DiskScheduler::Schedule() 来调度请求。调度程序维护一个后台工作线程，
  该线程使用磁盘管理器处理调度的请求。后台线程在 DiskScheduler 构造函数中创建，并加入其析构函数中。
 */
class DiskScheduler {
 public:
  explicit DiskScheduler(DiskManager *disk_manager);
  ~DiskScheduler();

  /**
   * TODO(P1): Add implementation
   *
   * @brief Schedules a request for the DiskManager to execute.
   * 安排 DiskManager 执行请求。
   * @param r The request to be scheduled. 待安排的请求。
   */
  void Schedule(DiskRequest r);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Background worker thread function that processes scheduled requests.
   *处理计划请求的后台工作线程函数。
   * The background thread needs to process requests while the DiskScheduler exists, i.e., this function should not
   * return until ~DiskScheduler() is called. At that point you need to make sure that the function does return.
  后台线程需要在 DiskScheduler 存在时处理请求，也就是说，此函数在调用 ~DiskScheduler()
  之前不应返回。此时您需要确保该函数确实返回。
  */
  void StartWorkerThread();

  using DiskSchedulerPromise = std::promise<bool>;

  /**
   * @brief Create a Promise object. If you want to implement your own version of promise, you can change this function
   * so that our test cases can use your promise implementation.
   * 创建一个 Promise 对象。如果你想实现自己的 Promise 版本，你可以更改此功能，以便我们的测试用例可以使用你的 Promise
   * 实现。
   * @return std::promise<bool>
   */
  auto CreatePromise() -> DiskSchedulerPromise {
    std::promise<bool> prom;
    return prom;
  };

  /**
   * @brief Increases the size of the database file to fit the specified number of pages.
   * 增加数据库文件的大小以适合指定的页数。
   * This function works like a dynamic array, where the capacity is doubled until all pages can fit.
   *该函数的工作方式类似于动态数组，其容量加倍直到可以容纳所有页面。
   * @param pages The number of pages the caller wants the file used for storage to support.
   * 调用者希望用于存储的文件支持的页数。
   */
  void IncreaseDiskSpace(size_t pages) { disk_manager_->IncreaseDiskSpace(pages); }

  /**
   * @brief Deallocates a page on disk.
   * 释放磁盘上的一个页面。
   * Note: You should look at the documentation for `DeletePage` in `BufferPoolManager` before using this method.
   * Also note: This is a no-op without a more complex data structure to track deallocated pages.
   *注意：在使用此方法之前，您应该查看“BufferPoolManager”中“DeletePage”的文档。
   *另请注意：如果没有更复杂的数据结构来跟踪已释放的页面，则此操作无效。
   * @param page_id The page ID of the page to deallocate from disk.
   */
  void DeallocatePage(page_id_t page_id) { disk_manager_->DeletePage(page_id); }

 private:
  /** Pointer to the disk manager. */
  DiskManager *disk_manager_ __attribute__((__unused__));
  /** A shared queue to concurrently schedule and process requests. When the DiskScheduler's destructor is called,
   * `std::nullopt` is put into the queue to signal to the background thread to stop execution.
   *一个共享队列，用于并发调度和处理请求。当调用 DiskScheduler 的析构函数时，
   *`std::nullopt` 被放入队列，以向后台线程发出信号停止执行。
   *
   */
  Channel<std::optional<DiskRequest>> request_queue_;
  /** The background thread responsible for issuing scheduled requests to the disk manager. */
  // 负责向磁盘管理器发出计划请求的后台线程。
  std::optional<std::thread> background_thread_;
};
}  // namespace bustub
