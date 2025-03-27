use std::sync::atomic::{AtomicUsize, Ordering};

/// A reference-counted pointer with generational counting to ensure memory safety.
///
/// `GenRef<T>` provides safe concurrent access to data by tracking both:
/// 1. The reference count (how many shared references exist)
/// 2. A generation number (incremented on each update)
///
/// This helps prevent the ABA problem in lock-free data structures, where a pointer is
/// changed from A to B and back to A, which might fool a compare-and-swap operation
/// into thinking the value hasn't changed when it has.
#[derive(Debug)]
pub struct GenRef<T> {
    /// The internal data being stored
    data: T,
    /// The reference count
    ref_count: AtomicUsize,
    /// The generation number - incremented on each update
    generation: AtomicUsize,
}

impl<T> GenRef<T> {
    /// Create a new `GenRef` with the given data.
    ///
    /// The initial reference count is 1, owned by the caller.
    /// The initial generation is 0.
    pub fn new(data: T) -> Self {
        GenRef {
            data,
            ref_count: AtomicUsize::new(1),
            generation: AtomicUsize::new(0),
        }
    }

    /// Get a reference to the inner data.
    pub fn get(&self) -> &T {
        &self.data
    }

    /// Get the current generation number.
    pub fn generation(&self) -> usize {
        self.generation.load(Ordering::Acquire)
    }

    /// Increment the reference count.
    pub fn inc_ref(&self) {
        self.ref_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the reference count.
    ///
    /// Returns true if this was the last reference.
    pub fn dec_ref(&self) -> bool {
        self.ref_count.fetch_sub(1, Ordering::Release) == 1
    }

    /// Update the data, returning the old data.
    ///
    /// This operation increments the generation number.
    pub fn update(&mut self, new_data: T) -> T {
        // Increment the generation number
        self.generation.fetch_add(1, Ordering::Release);

        // Replace the data
        std::mem::replace(&mut self.data, new_data)
    }
}

impl<T: Clone> GenRef<T> {
    /// Get a clone of the data.
    pub fn clone_data(&self) -> T {
        self.data.clone()
    }
}

/// A handle to a generationally reference-counted object.
///
/// When this handle is dropped, the reference count is decremented.
/// If this is the last reference, the wrapped object will be dropped.
#[derive(Debug)]
pub struct GenRefHandle<T> {
    /// The generational reference
    gen_ref: *const GenRef<T>,
    /// The generation when this handle was created
    generation: usize,
}

// Safe to send handles across threads as long as T is Send
unsafe impl<T: Send> Send for GenRefHandle<T> {}
// Safe to share handles across threads as long as T is Sync
unsafe impl<T: Sync> Sync for GenRefHandle<T> {}

impl<T> GenRefHandle<T> {
    /// Create a new handle from a boxed `GenRef`.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `gen_ref` points to a valid `GenRef<T>` that will remain valid
    /// until all `GenRefHandle`s referencing it are dropped.
    pub unsafe fn new(gen_ref: *const GenRef<T>) -> Self {
        // Increment reference count
        unsafe {
            (*gen_ref).inc_ref();
        }

        // Get current generation
        let generation = unsafe { (*gen_ref).generation() };

        GenRefHandle {
            gen_ref,
            generation,
        }
    }

    /// Get a reference to the data.
    ///
    /// This is safe because the ref-counting ensures the pointer remains valid.
    pub fn get(&self) -> &T {
        unsafe { (*self.gen_ref).get() }
    }

    /// Check if the generation has changed since this handle was created.
    pub fn is_stale(&self) -> bool {
        let current_gen = unsafe { (*self.gen_ref).generation() };
        current_gen != self.generation
    }

    /// Get the generation of this handle.
    pub fn generation(&self) -> usize {
        self.generation
    }
}

impl<T: Clone> GenRefHandle<T> {
    /// Clone the data this handle points to.
    pub fn clone_data(&self) -> T {
        unsafe { (*self.gen_ref).clone_data() }
    }
}

impl<T> Clone for GenRefHandle<T> {
    fn clone(&self) -> Self {
        // Create a new handle, which will increment the ref count
        unsafe { GenRefHandle::new(self.gen_ref) }
    }
}

impl<T> Drop for GenRefHandle<T> {
    fn drop(&mut self) {
        // Decrement reference count
        unsafe {
            // If this returns true, we were the last reference
            if (*self.gen_ref).dec_ref() {
                // This was the last reference, so deallocate the GenRef
                let _ = Box::from_raw(self.gen_ref as *mut GenRef<T>);
            }
        }
    }
}

/// Creates a new `GenRef` object and returns a handle to it.
pub fn make_gen_ref<T>(data: T) -> GenRefHandle<T> {
    // Allocate a GenRef on the heap
    let gen_ref = Box::new(GenRef::new(data));
    // Convert the Box to a raw pointer to avoid double-free
    let raw_ptr = Box::into_raw(gen_ref);
    // Create a handle to it - safe to call unsafe function here as we just created the pointer
    unsafe { GenRefHandle::new(raw_ptr) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn test_gen_ref_basic() {
        let handle = make_gen_ref(42);
        assert_eq!(*handle.get(), 42);
        assert_eq!(handle.generation(), 0);
        assert!(!handle.is_stale());
    }

    #[test]
    fn test_gen_ref_clone() {
        let handle1 = make_gen_ref(42);
        let handle2 = handle1.clone();

        assert_eq!(*handle1.get(), 42);
        assert_eq!(*handle2.get(), 42);
    }

    #[test]
    fn test_gen_ref_threaded() {
        let handle = make_gen_ref(vec![1, 2, 3]);
        let handle = Arc::new(handle);

        let barrier = Arc::new(Barrier::new(8));
        let mut handles = vec![];

        for _ in 0..8 {
            let handle_clone = handle.clone();
            let barrier_clone = barrier.clone();

            let thread_handle = thread::spawn(move || {
                barrier_clone.wait();
                let data = handle_clone.clone_data();
                assert_eq!(data, vec![1, 2, 3]);
            });

            handles.push(thread_handle);
        }

        for h in handles {
            h.join().unwrap();
        }
    }
}
