use std::{fs::{self, File, OpenOptions}, io, path::Path};

use rustler::NifResult;
use zip::ZipArchive;

use crate::Yes;

fn good_filename(s: &str) -> bool {
    !s.starts_with('/') &&  // exclude any absolute paths
        !s.ends_with('/') &&  // exclude any directories
        !s.starts_with("../") && // exclude any files that say "I belong in the parent dir"
        !s.contains("/../") // exclude any files that try to traverse upward
}

#[rustler::nif(schedule = "DirtyCpu")]
fn unzip_table(path: &str) -> Result<Vec<String>, &'static str> {
    let r = File::open(path).map_err(|_| "Unable to open zip file")?;
    let files = ZipArchive::new(r).map_err(|_| "Unable to read zip file")?
        .file_names()
        .filter(|s| good_filename(s))
        .map(str::to_owned)
        .collect();
    Ok(files)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn unzip_files(zip_filename: &str, target_dir: &str) -> Result<(), &'static str> {
    let target_dir = Path::new(target_dir);
    let r = File::open(zip_filename).map_err(|_| "Unable to open zip file")?;
    let mut zip = ZipArchive::new(r).map_err(|_| "Unable to read zip file")?;
    for i in 0..zip.len() {
        let mut file = zip.by_index(i).unwrap();
        let filename = file.name();
        if !good_filename(filename) {
            continue;
        }
        let target = target_dir.join(filename);
        if let Some(p) = target.parent() {
            fs::create_dir_all(p).map_err(|_| "Unable to create temp dir")?;
        }
        let mut outfile = OpenOptions::new().write(true).create_new(true).open(target).map_err(|_| "Unable to create temp file")?;
        io::copy(&mut file, &mut outfile).map_err(|_| "Unable to copy data")?;
        outfile.sync_all().map_err(|_| "Unable to finish write")?;
    }
    Ok(())
}
