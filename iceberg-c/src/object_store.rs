use std::{
    ffi::{c_char, CStr},
    sync::Arc,
};

use object_store::{aws::AmazonS3Builder, ObjectStore};

pub struct CObjectStore(pub Arc<dyn ObjectStore>);

/// Constructor for aws object_store
#[no_mangle]
pub extern "C" fn object_store_new_aws(
    region: *const c_char,
    bucket: *const c_char,
    access_token: *const c_char,
) -> Box<CObjectStore> {
    let region = unsafe { CStr::from_ptr(region) };
    let bucket = unsafe { CStr::from_ptr(bucket) };
    let access_token = unsafe { CStr::from_ptr(access_token) };

    let object_store: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_region(region.to_str().unwrap())
            .with_bucket_name(bucket.to_str().unwrap())
            .with_token(access_token.to_str().unwrap())
            .build()
            .expect("Failed to create aws object store"),
    );
    Box::new(CObjectStore(object_store))
}

/// Free object store memory
#[no_mangle]
pub extern "C" fn object_store_free(_object_store: Option<Box<CObjectStore>>) {}
