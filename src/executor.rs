use crate::task::{Task, TaskId};
use std::{collections::BTreeMap, sync::Arc};
use core::task::Waker;
use std::task::{Context, Poll, Wake};
use crossbeam_queue::ArrayQueue;

pub struct Executor {
    tasks: BTreeMap<TaskId, Task>,
    queue: Arc<ArrayQueue<TaskId>>,
    waker_cache: BTreeMap<TaskId, Waker>
}

impl Executor {
    pub fn new() -> Self {
        Self {
            tasks: BTreeMap::new(),
            queue: Arc::new(ArrayQueue::new(128)),
            waker_cache: BTreeMap::new()
        }
    }

    pub fn spawn(&mut self, task: Task) {
        let id = task.id;
        if self.tasks.insert(task.id, task).is_some() {
            panic!("task with same id already in list");
        }

        self.queue.push(id).expect("queue full");
    }

    pub fn run(&mut self) {
        loop {
            if self.tasks.is_empty() {
                break;
            }

            self.run_ready_tasks();
        }
    }

    fn run_ready_tasks(&mut self) {
        let Self {
            tasks,
            queue,
            waker_cache
        } = self;

        while let Some(id) = queue.pop() {
            let task = match tasks.get_mut(&id) {
                Some(t) => t,
                None => continue
            };

            let waker = waker_cache
                .entry(id)
                .or_insert_with(|| TaskWaker::new(id, queue.clone()));

            let mut context = Context::from_waker(waker);
            match task.poll(&mut context) {
                Poll::Ready(()) => {
                    tasks.remove(&id);
                    waker_cache.remove(&id);
                }
                Poll::Pending => {}
            }
        }
    }
}

struct TaskWaker {
    id: TaskId,
    queue: Arc<ArrayQueue<TaskId>>
}

impl TaskWaker {
    fn new(id: TaskId, queue: Arc<ArrayQueue<TaskId>>) -> Waker {
        Waker::from(Arc::new(TaskWaker {
            id,
            queue
        }))
    }

    fn wake_task(&self) {
        self.queue.push(self.id).expect("task queue full")
    }
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.wake_task()
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wake_task()
    }
}