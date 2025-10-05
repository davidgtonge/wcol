#[no_mangle]
pub unsafe extern "C" fn alloc_bytes(len: usize) -> *mut u8 {
    use std::alloc::{alloc, Layout};
    let layout = Layout::from_size_align(len, std::mem::align_of::<u8>()).unwrap();
    alloc(layout)
}

#[no_mangle]
pub unsafe extern "C" fn free_bytes(ptr: *mut u8, len: usize) {
    use std::alloc::{dealloc, Layout};
    let layout = Layout::from_size_align(len, std::mem::align_of::<u8>()).unwrap();
    dealloc(ptr, layout);
}
