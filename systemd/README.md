# systemd service and timer

Copy these files into /etc/systemd/system/

Enable at boot

`# systemctl enable /etc/systemd/system/jhu-data-parser.timer`

Start the timer

`# systemctl start /etc/systemd/system/jhu-data-parser.timer`