# systemd service and timer

Copy these files into /etc/systemd/system/

Enable at boot

`# systemctl enable jhu-data-parser.timer`

Start the timer

`# systemctl start jhu-data-parser.timer`