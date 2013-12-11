uwsgi-pgnotify
==============

Maps PostgreSQL notification system to uWSGI signal framework

Installation
============

```
git clone https://github.com/unbit/uwsgi-pgnotify
uwsgi --buid-plugin uwsgi-pgnotify
```

Configuration
=============

raise signal 17 whenever postgresql server on localhost notify channel FOOBAR

```ini
[uwsgi]
plugins = pgnotify
pgnotify-signal = 17 FOOBAR user=postgres
...
```

