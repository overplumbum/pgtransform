#!/usr/bin/env python
from pgtransform import run
import sys
from hashlib import sha1
import uuid

SALT = None

def hashed_mail(email):
	parts = email.split('@', 1)
	if len(parts) < 2:
		return sha1(SALT + email).hexdigest()[:10]
	else:
		login, domain = parts
		domain = 'devnull.ostrovok.ru'
		login = sha1(SALT + email).hexdigest()[:10]
		return login + '@' + domain

def update_auth_user(row):
	row['email'] = hashed_mail(row['email'])
	row['username'] = hashed_mail(row['username'])
	row['password'] = '!'


transformations = {
	'auth_user': update_auth_user,
}

if __name__ == '__main__':
	SALT = str(uuid.uuid1())
	run(sys.argv[1], transformations)
