#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import os
import sys


project_path = os.path.split(os.path.realpath(__file__))[0] + '/../'
if project_path not in sys.path:
    sys.path.append(project_path)


def main():
    """Run administrative tasks."""
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dashboard.settings')
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    main()
