#!/bin/bash
# Run with: sudo bash setup_service.sh

printf '[Unit]\nDescription=Rollabot TeamSpeak Bot\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nType=simple\nUser=pi\nWorkingDirectory=/home/pi/Projs/rollabot\nExecStartPre=/usr/bin/git pull\nExecStart=/home/pi/.local/bin/uv run ini.py\nRestart=on-failure\nRestartSec=15\n\n[Install]\nWantedBy=multi-user.target\n' > /etc/systemd/system/rollabot.service

systemctl daemon-reload
systemctl enable rollabot
systemctl start rollabot

(crontab -l 2>/dev/null; echo "0 0 * * * systemctl restart rollabot") | crontab -

echo "Done!"
systemctl status rollabot --no-pager
