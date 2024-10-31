
(**

# Constraints & Relationships

A typical relational database will have many connected tables and views 
through foreign key constraints.  The SQL provider can show you these 
constraints on entities.  They appear as properties named the same as the 
constraint in the database.

You can gain access to these child or parent entities by simply enumerating 
the property in question.

While SQL provider automatically generates getters from foreign-key relations, it doesn't have (yet) any automatic support for creating a properly ordered graph of related entities in a single transaction. However, SQL provider submits entities to a database in the same order they were created. As long as you create entities in their dependency order, you won't get foreign-key constraint violations.

*)
