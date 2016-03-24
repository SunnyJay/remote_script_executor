drop table if exists entries;
create table entries (
  id integer primary key autoincrement,
  title text not null,
  toolpath text not null,
  content text not null,
  auth integer not null,
  showtag integer not null,
  addtime text  not null
);