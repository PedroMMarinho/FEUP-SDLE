
CREATE TABLE IF NOT EXISTS ShoppingList (
    uuid UUID NOT NULL,      
    name TEXT NOT NULL,
    crdt JSONB NOT NULL,
    logical_clock INTEGER DEFAULT 0,
    isReplica BOOLEAN NOT NULL,  -- whether this is a replica of a list from another server
    replicaID INTEGER DEFAULT 0,
    PRIMARY KEY (uuid, replicaID)          
);

CREATE TABLE IF NOT EXISTS ShoppingListItem (
    id SERIAL PRIMARY KEY,
    shopping_list_uuid UUID NOT NULL,
    shopping_list_replicaID INTEGER NOT NULL,
    name TEXT NOT NULL,
    quantityNeeded INTEGER NOT NULL,
    quantityAcquired INTEGER NOT NULL,
    position INTEGER NOT NULL,
    CHECK (quantityNeeded >= 0 AND quantityAcquired >= 0),
    FOREIGN KEY (shopping_list_uuid, shopping_list_replicaID) 
        REFERENCES ShoppingList(uuid, replicaID) 
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_shopping_list_uuid 
ON ShoppingListItem (shopping_list_uuid, shopping_list_replicaID);