CREATE TABLE IF NOT EXISTS ShoppingList (
    uuid UUID PRIMARY KEY,      
    name TEXT NOT NULL,
    crdt JSONB NOT NULL,
    logical_clock INTEGER DEFAULT 0,
    notSent BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS ShoppingListItem (
    id SERIAL PRIMARY KEY,
    shopping_list_uuid UUID NOT NULL REFERENCES ShoppingList(uuid) ON DELETE CASCADE,
    name TEXT NOT NULL,
    quantityNeeded INTEGER NOT NULL,
    quantityAcquired INTEGER NOT NULL,
    position INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_shopping_list_uuid 
ON ShoppingListItem (shopping_list_uuid);