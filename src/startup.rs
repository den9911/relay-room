use anyhow::{Context,Result};
use std::{fs::OpenOptions,net::{Ipv4Addr,TcpListener},path::Path,time::{SystemTime,UNIX_EPOCH}};
pub fn check(data:Option<&Path>,port:Option<u16>)->Result<()> {
 if let Some(path)=data {
  let parent=path.parent().filter(|p|!p.as_os_str().is_empty()).unwrap_or(Path::new("."));
  anyhow::ensure!(parent.is_dir(),"Data directory does not exist; create it or choose another database path / Создайте каталог данных");
  if path.exists(){let _file=OpenOptions::new().write(true).open(path).context("Database is not writable; check permissions / Проверьте права базы")?;}
  let nonce=SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
  let probe=parent.join(format!(".startup-{}-{nonce}",std::process::id()));
  let file=OpenOptions::new().write(true).create_new(true).open(&probe).context("Data directory is not writable / Каталог данных недоступен для записи")?;
  drop(file);std::fs::remove_file(probe)?;
 }
 if let Some(port)=port{let _listener=TcpListener::bind((Ipv4Addr::LOCALHOST,port)).context("Local port unavailable; stop the other instance or check permissions / Локальный порт занят или недоступен")?;}
 println!("Startup checks passed; no server or network requests started / Проверки запуска пройдены");Ok(())
}
