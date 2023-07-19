ALTER TABLE
    id_and_name ADD COLUMN a_new_column VARCHAR;

CREATE
    TABLE
        a_new_table(
            id INTEGER NOT NULL
        );

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
        'contents-of-the-new-column'
    );

INSERT
    INTO
        a_new_table(id)
    VALUES(1)