#!/bin/bash

psql -h localhost -p 15432 -U admin tcscala -c "

create table meetup (
  id uuid primary key,
  title text not null,
  summary text,
  attendees integer not null,
  pizzas integer
);

create table needs_summary (
  id uuid primary key,
  meetup_id uuid not null
);

insert into meetup (id, title, summary, attendees, pizzas) values ('3911cc14-7dae-4d83-8d2e-e953216db476', 'FP to the Max', null, 9, null);

insert into meetup (id, title, summary, attendees, pizzas) values ('d953788c-de72-4c8b-a991-1aef1ae7c9ec', 'Gatling Load Testing', 'Agenda: I am new to gating myself...', 5, null);

insert into meetup (id, title, summary, attendees, pizzas) values ('e778e1a7-671a-40da-b809-1f0847c1f2cc', 'Minne Scala Days', 'Agenda: Scala days is happening this week...', 6, 3);

insert into meetup (id, title, summary, attendees, pizzas) values ('8779f24b-9aa6-4524-bb2c-492bcfac6f83', 'GraphQL and Dotty', null, 13, null);

insert into meetup (id, title, summary, attendees, pizzas) values ('3a888cac-55c5-47af-afe0-419774b0b46c', 'Hack Night', null, 9, null);

insert into meetup (id, title, summary, attendees, pizzas) values ('d04a2d8a-7e3c-4b97-9b81-93818bbfc3e1', 'Akka!', 'Agenda: We will discuss the Akka Actors...', 16, null);
"
