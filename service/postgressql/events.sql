CREATE TABLE IF NOT EXISTS events (
    "id" varchar(36) PRIMARY KEY DEFAULT gen_random_uuid(),
    "domain" text,
    "delivered" integer,
    "bounced" integer,
    "updatedAt" timestamp with time zone DEFAULT now(),
);

