extern crate version_check;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    if cfg!(unix){
        println!("cargo:rustc-cfg=use_ino");
    } else if version_check::is_feature_flaggable().unwrap_or(false) && cfg!(windows) {
        println!("cargo:rustc-cfg=use_windows_file_numbers");
    } else{
        println!("cargo:rustc-cfg=use_ino_placeholder");
    }
}