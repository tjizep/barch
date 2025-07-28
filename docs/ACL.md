### ACL command

This command can be used to add,remove and configure users.
The acl data is stored separately from the main and does not get replicated.
`ACL` can only be used on the RESP interface defaulted to port 14000.
A special `all` flag can be used to grant or remove ALL permissions.
Available options are `GETUSER`,`SETUSER` and `DEL`

See examples below for usage.

### AUTH command

Upon connection the `default` user is authenticated - if no rights are supplied then all rights are assumed.
`ACL` can be used to reduce rights of the default user.

### AUTH restrict `default`

The default password for the default user is `empty` if this password is changed no useful connection is possible because `AUTH` cannot succeed.
it would be better to use `ACL SETUSER default -all +auth`

#### *DO NOT USE* `ACL SETUSER default on >empty` 

### ACL Categories
There are thirteen (13) categories to choose from when creating user permissions:
1. read : distinguishes read-only commands (GET,HGET,LFRONT,ZGET etc.) in combination with hash,keys,orderset,list
2. write : distinguishes write as above (SET,HSET,LPOP,LPUS etc.)
3. keys : for standard key commands (GET,SET,ADD etc)
4. data : all users have this right
5. hash : for hash set access the H* calls 
6. orderedset : for Z* calls
7. list : for L* calls
8. connection : for all rpc, outgoing remote connection access
9. config : set config values
10. dangerous : commands like LOAD, CLEAR (the KEYS command isn't blocking in BARCH so not dangerous)
11. keyspace : not used
12. stats : statistics
13. auth : ability to authenticate (added by default can be removed)

### Example showing how to add set and remove a user
The `>` sign is used to set the user secret, the user edited is `krease`
```redis
> ACL GETUSER krease
(nil)
> ACL SETUSER krease on >test123 +read +write +keys +orderedset +hash +connection +list
OK
> ACL GETUSER krease
 1) "connection"
 2) true
 3) "data"
 4) true
 5) "hash"
 6) true
 7) "keys"
 8) true
 9) "list"
10) true
11) "orderedset"
12) true
13) "read"
14) true
15) "write"
16) true
> ACL SETUSER krease on -list
OK
> ACL GETUSER krease
 1) "connection"
 2) true
 3) "data"
 4) true
 5) "hash"
 6) true
 7) "keys"
 8) true
 9) "list"
10) false
11) "orderedset"
12) true
13) "read"
14) true
15) "write"
16) true
> ACL DEL krease
OK
> ACL GETUSER krease
(nil)

```