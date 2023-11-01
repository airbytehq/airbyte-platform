ALTER TABLE
    id_and_name ADD COLUMN a_new_column INTEGER NOT NULL DEFAULT(50);

INSERT
    INTO
        id_and_name(
            id,
            name,
            a_new_column
        )
    VALUES(
        6,
        'a-new-name',
        100
    );