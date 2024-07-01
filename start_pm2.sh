#!/bin/bash

source /app/www/vhosts/labchat.tnt.co.th/httpdocs-service/venv/bin/activate 

pm2 del service-affiliate
pm2 start