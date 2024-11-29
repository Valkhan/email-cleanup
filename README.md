# email-cleanup
Python script to cleanup emails from a spreadsheet validating syntax errors and excluding some keywords.

## Installation
```ps
python -m venv email-cleanup
.\email-cleanup\Scripts\Activate
pip install -r requirements.txt
deactivate
```

## Execution

```ps
python cleanup input.csv output.xlsx
```

## Improvements

- [ ] Add .env file to store target field name or argument
- [ ] Add feature to define the csv separator mode "," or ";" [currently only ";" is supported]
- [ ] Add feature to check entry file mode (csv, xlsx) [currently only csv is supported]
- [ ] Add feature to check set output file mode (csv, xlsx) [currently only xlsx is supported]
- [ ] Separate the unwanted_terms variable into a file
- [ ] Add feature to remove blockedlist
- [ ] Strip last '.' in emails.
- [ ] Similiar providers hotmial.com to hotmail.com
- [ ] Generate log with suspect test/wrong emails


