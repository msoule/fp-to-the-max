#!/bin/bash

psql -h localhost -p 15432 -U admin tcscala -c "

drop table meetup;
drop table needs_summary;
"
