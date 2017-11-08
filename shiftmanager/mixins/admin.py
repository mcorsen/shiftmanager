import functools
import random
import string


def random_password(length=64):
    """Return a strong password valid for Redshift.

    Constraints:

    * 8 to 64 characters in length.
    * Must contain at least one uppercase letter, one lowercase letter,
      and one number.
    * Can use any printable ASCII characters (ASCII code 33 to 126)
      except ``'`` (single quote), ``\"`` (double quote), ``\\``, ``/``,
      ``@``, ``\``` or space.
    * See `Redshift's CREATE USER docs
      <http://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_USER.html>`_
    """
    rand = random.SystemRandom()
    invalid_chars = r'''`\/'"@ '''
    valid_chars_set = set(string.digits + string.ascii_letters +
                          string.punctuation) - set(invalid_chars)
    valid_chars = list(valid_chars_set)
    chars = [
        rand.choice(string.ascii_uppercase),
        rand.choice(string.ascii_lowercase),
        rand.choice(string.digits)
    ]
    chars += [rand.choice(valid_chars) for x in range(length - 3)]
    rand.shuffle(chars)
    return ''.join(chars)


class AdminMixin(object):
    """User administration base class for `Redshift`."""

    @staticmethod
    @functools.wraps(random_password)
    def random_password(length=64):
        return random_password(length)

    def create_user(self,
                    name,
                    password,
                    valid_until=None,
                    createdb=False,
                    createuser=False,
                    groups=None,
                    execute=False,
                    **parameters):
        """Return a SQL str defining a new user.

        Parameters
        ----------
        name : str
            The name of the user account to create.
        password : str
            The password for the new user.
        valid_until : str or datetime
            An absolute time after which the user account password
            is no longer valid.
        createdb : boolean
            Allow the new user account to create databases.
        createuser : boolean
            Create a superuser with all database privileges.
        groups : list of str
            Existing groups that the user will belong to.
        execute : boolean
            Execute the command in addition to returning it.
        parameters :
            Additional keyword arguments are interpreted as configuration
            parameters whose values will be set by additional ALTER USER
            statements added to the batch.
        """
        data = dict(password=password, valid_until=str(valid_until))
        statement = "CREATE USER %s" % name
        if createdb:
            statement += " CREATEDB"
        if createuser:
            statement += " CREATEUSER"
        if groups:
            statement += " IN GROUP "
            statement += ', '.join(groups)
        statement += " PASSWORD %(password)s"
        if valid_until:
            statement += " VALID UNTIL %(valid_until)s"
        if parameters:
            statement += ';\n' + self.alter_user(name, **parameters)
        return self.mogrify(statement, data, execute)

    def alter_user(self,
                   name,
                   password=None,
                   valid_until=None,
                   createdb=None,
                   createuser=None,
                   rename=None,
                   execute=False,
                   **parameters):
        """Return a SQL str that alters an existing user.

        Parameters
        ----------
        name : str
            The name of the user account to create.
        password : str
            The password for the new user.
        valid_until: str or datetime
            An absolute time after which the user account password
            is no longer valid.
        createdb : boolean
            Allow the new user account to create databases.
        createuser : boolean
            Create a superuser with all database privileges.
        rename : str
            New name to assign the user.
        execute : boolean
            Execute the command in addition to returning it.
        parameters :
            Additional keyword arguments are interpreted as configuration
            parameters whose values will be set by additional ALTER USER
            statements added to the batch. For values set to None, the
            parameter will be reset, letting system defaults take effect.
        """
        data = dict(password=password, valid_until=valid_until)
        statement = "ALTER USER %s " % name
        options = []
        if password:
            options.append("PASSWORD %(password)s")
        if createdb is not None:
            if createdb:
                options.append("CREATEDB")
            else:
                options.append("NOCREATEDB")
        if createuser is not None:
            if createuser:
                options.append("CREATEUSER")
            else:
                options.append("NOCREATEUSER")
        if rename:
            options.append("RENAME TO %s" % rename)
        for param, value in parameters.items():
            if value is None:
                options.append("RESET %s" % param)
            else:
                options.append("SET %s = %s" % (param, value))
        statement += ' '.join(options)
        return self.mogrify(statement, data, execute)
