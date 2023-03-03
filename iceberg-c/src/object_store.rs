use std::{
    ffi::{c_char, CStr},
    sync::Arc,
};

use object_store::{aws::AmazonS3Builder, ObjectStore};

pub struct ArcObjectStore(pub Arc<dyn ObjectStore>);

/// Constructor for aws object_store with an access token
#[no_mangle]
pub extern "C" fn object_store_new_aws_token(
    region: *const c_char,
    bucket: *const c_char,
    access_token: *const c_char,
) -> Box<ArcObjectStore> {
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
    Box::new(ArcObjectStore(object_store))
}

/// Constructor for aws object_store with an access_key_id and secret_access_key
#[no_mangle]
pub extern "C" fn object_store_new_aws_access_key(
    region: *const c_char,
    bucket: *const c_char,
    access_key_id: *const c_char,
    secret_access_key: *const c_char,
) -> Box<ArcObjectStore> {
    let region = unsafe { CStr::from_ptr(region) };
    let bucket = unsafe { CStr::from_ptr(bucket) };
    let access_key_id = unsafe { CStr::from_ptr(access_key_id) };
    let secret_access_key = unsafe { CStr::from_ptr(secret_access_key) };

    let object_store: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_region(region.to_str().unwrap())
            .with_bucket_name(bucket.to_str().unwrap())
            .with_access_key_id(access_key_id.to_str().unwrap())
            .with_secret_access_key(secret_access_key.to_str().unwrap())
            .build()
            .expect("Failed to create aws object store"),
    );
    Box::new(ArcObjectStore(object_store))
}

/// Free object store memory
#[no_mangle]
pub extern "C" fn object_store_free(_object_store: Option<Box<ArcObjectStore>>) {}
