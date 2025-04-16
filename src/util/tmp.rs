use std::fs::{self, File};

use super::{common::rng_str, error::SubResult};

pub struct TempFile {
    pub filepath: String,
}

impl TempFile {
    pub fn new(filepath: &str) -> SubResult<TempFile> {
        match File::create(filepath) {
            Ok(x) => Ok(TempFile { filepath: filepath.to_owned(), }),
            Err(e) => Err(e.to_string()),
        }
    }
    pub fn get(&self) -> Result<File, std::io::Error> {
        File::open(&self.filepath)
    }
    pub fn get_mut(&self) -> Result<File, std::io::Error> {
        File::create(&self.filepath)
    }
}

// TODO: move to CpDefault where it returns a wrapped result
impl Default for TempFile {
    fn default() -> Self {
        let rndstr = rng_str(12);
        let filepath = format!("/tmp/{}", &rndstr);
        TempFile::new(&filepath).unwrap()
    }

}

impl Drop for TempFile {
    fn drop(&mut self) {
        fs::remove_file(&self.filepath).expect(format!("Failed to delete TempFile {}", &self.filepath).as_str());
    }
}

#[cfg(test)]
mod tests {
    use core::time;
    use std::{fs, io::{Read, Write}, thread::sleep};

    use super::TempFile;

    #[test]
    fn valid_create_write_delete() {
        let tf = TempFile::new("/tmp/__valid_create_write_delete_7862af50be.log").unwrap();
        {
            let mut filehandle  = tf.get_mut().unwrap();
            filehandle.write_all( b"Lorem ipsum").unwrap();
        }
        {
            let mut filehandle  = tf.get().unwrap();
            let mut contents = String::new();
            filehandle.read_to_string(&mut contents).unwrap();
            assert_eq!(contents, "Lorem ipsum");
        }
    }

    #[test]
    fn fail_to_delete() {
        let fp = "/tmp/__fail_to_delete_7862af50be.log";
        {
            let tf = TempFile::new(fp).unwrap();
        }
        assert!(!fs::exists(fp).unwrap());

    }
}
