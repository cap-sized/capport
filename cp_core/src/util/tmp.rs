use std::{env::temp_dir, fs::{self, File}};

use log::info;

use super::{
    common::rng_str,
    error::{CpError, CpResult},
};

#[derive(Debug)]
pub struct TempFile {
    pub filepath: String,
}

impl TempFile {
    pub fn new(filepath: &str) -> CpResult<TempFile> {
        match File::create(filepath) {
            Ok(_) => Ok(TempFile {
                filepath: filepath.to_owned(),
            }),
            Err(e) => Err(CpError::RawError(e)),
        }
    }
    pub fn default_in_dir(dir: &str, ext: &str) -> CpResult<TempFile> {
        let rndstr = rng_str(12);
        let filepath = format!("{}/{}.{}", dir, &rndstr, ext);
        info!("temp filepath created: {}", &filepath);
        // sleep(time::Duration::from_secs(2000));
        TempFile::new(&filepath)
    }
    pub fn get(&self) -> Result<File, std::io::Error> {
        File::open(&self.filepath)
    }
    pub fn get_mut(&self) -> Result<File, std::io::Error> {
        File::create(&self.filepath)
    }
}

impl Default for TempFile {
    fn default() -> Self {
        let tmp_dir = temp_dir();
        let rndstr = rng_str(12);
        let filepath = format!("{}/{}", tmp_dir.to_str().unwrap(), &rndstr);
        TempFile::new(&filepath).unwrap()
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        fs::remove_file(&self.filepath).unwrap_or_else(|e| match e.kind() {
            std::io::ErrorKind::NotFound => {}
            other => panic!("Failed to delete TempFile {}: {:?}", &self.filepath, other),
        });
    }
}

#[cfg(test)]
mod tests {

    use std::{
        fs,
        io::{Read, Write},
    };

    use super::TempFile;

    #[test]
    fn valid_create_write_delete() {
        let tf = TempFile::default();
        {
            let mut filehandle = tf.get_mut().unwrap();
            filehandle.write_all(b"Lorem ipsum").unwrap();
        }
        {
            let mut filehandle = tf.get().unwrap();
            let mut contents = String::new();
            filehandle.read_to_string(&mut contents).unwrap();
            assert_eq!(contents, "Lorem ipsum");
        }
    }

    #[test]
    fn check_delete_on_drop() {
        let fp = "/tmp/__delete_on_drop_7862af50be.log";
        {
            TempFile::new(fp).unwrap();
        }
        assert!(!fs::exists(fp).unwrap());
    }

    #[test]
    fn invalid_file_no_dir() {
        let fp = "/tmp/__nondir_7862/__fail_to_delete_7862af50be.log";
        TempFile::new(fp).unwrap_err();
    }
}
