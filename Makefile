VENV := $(CURDIR)/venv
export PATH := $(VENV)/bin:$(PATH)

test: install
	flake8 *.py
	$(VENV)/bin/pip install pytest
	$(VENV)/bin/pytest

install: $(VENV)
	$(VENV)/bin/pip install -r requirements.txt
	$(VENV)/bin/python setup.py develop

$(VENV):
	virtualenv $@

requirements.txt:
	$(VENV)/bin/pip freeze > $@
