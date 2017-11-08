VENV := $(CURDIR)/venv
export PATH := $(VENV)/bin:$(PATH)

test: install
	flake8 *.py

install: $(VENV)
	$(VENV)/bin/pip install -r requirements.txt

$(VENV):
	virtualenv $@

requirements.txt:
	$(VENV)/bin/pip freeze > $@
