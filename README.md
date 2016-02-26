This python class is designed to provide database abstraction for 
sqlite.  Abstracting commonly used database actions into an easy-to-
use class.  Most all methods should be self documenting.  Below
are a quick list of features:

* Create Table
* Drop Table
* Insert 
  * May be called several ways, as a tuple of just values or a 
dictionary.
* Get record by "id"
* Search record(s)
  * Search criteria as array of 3-tuple (column, comparator, value)
* Update record(s) - passing dictionary of update values and 
a criteria 3-tuple as defined in search
* Delete Record(s) criteria passed as array of 3-tuple like search.

Feel free to copy / contribute and hack.  If you modify or enhance,
send me a pull request, I will happily merge your changes back in.

Note:  I have plans to extend this a bit further to allow making
DB operation using a python object instead of a dictionary. The object
will need to implement a to_dict() and from_dict() (preferred), or
get_var() / set_var() or as a last resort look for the variable names
that match the column names in the database (with optional leading 
underscore.)

Enjoy. 