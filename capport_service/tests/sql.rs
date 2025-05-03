use capport_service::service::sql::HasSqlClient;
use capport_service::{
    context::service::{DefaultSvcConfig, DefaultSvcDistributor},
    service::sql::{SqlClient, SqlClientConfig},
};
use serde::{Deserialize, Serialize};
use sqlx::Postgres;

const SQL_URI: &str = "postgres://postgres:postgres@localhost/csdb";

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, sqlx::FromRow)]
struct TestPerson {
    name: String,
    id: i32,
    desc: String,
}

#[tokio::test(flavor = "multi_thread")]
async fn valid_default_config_sql() {
    let sql_config = SqlClientConfig::new(SQL_URI, 1, 1);
    let sql_client = SqlClient::new(sql_config.clone()).await.unwrap();
    let pool = sql_client.get_pool().unwrap();

    sqlx::query(
        r#"
            CREATE TABLE IF NOT EXISTS persons (
                name TEXT NOT NULL,
                id INTEGER PRIMARY KEY,
                "desc" TEXT NOT NULL
            )
            "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        r#"
            DELETE FROM persons
            "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        r#"
        INSERT INTO persons (name, id, "desc")
        VALUES ($1, $2, $3), ($4, $5, $6)
        "#,
    )
    .bind("foo")
    .bind(1)
    .bind("bar")
    .bind("dee")
    .bind(2)
    .bind("bee")
    .execute(&pool)
    .await
    .unwrap();

    let actual = sqlx::query_as::<Postgres, TestPerson>(r#"SELECT name, id, "desc" FROM persons ORDER BY id"#)
        .fetch_all(&pool)
        .await
        .unwrap();
    let expected = vec![
        TestPerson {
            name: "foo".to_owned(),
            id: 1,
            desc: "bar".to_owned(),
        },
        TestPerson {
            name: "dee".to_owned(),
            id: 2,
            desc: "bee".to_owned(),
        },
    ];
    assert_eq!(&actual, &expected);
    {
        let mut svc = DefaultSvcDistributor {
            config: DefaultSvcConfig {
                mongo: None,
                sql: Some(sql_config.clone()),
            },
            mongo_client: None,
            sql_client: None,
        };

        svc.setup(&["sql"]).unwrap();
        let actual = sqlx::query_as::<Postgres, TestPerson>(r#"SELECT name, id, "desc" FROM persons ORDER BY id"#)
            .fetch_all(&svc.get_pool().unwrap())
            .await
            .unwrap();

        assert_eq!(&actual, &expected);

        sqlx::query(
            r#"
            DELETE FROM persons
            "#,
        )
        .execute(&svc.get_pool().unwrap())
        .await
        .unwrap();
        let final_actual =
            sqlx::query_as::<Postgres, TestPerson>(r#"SELECT name, id, "desc" FROM persons ORDER BY id"#)
                .fetch_all(&svc.get_pool().unwrap())
                .await
                .unwrap();
        assert!(&final_actual.is_empty());
    }
}

#[test]
fn test_setup_svc_blocking() {
    let sql_config = SqlClientConfig::new(SQL_URI, 1, 1);
    let mut svc = DefaultSvcDistributor {
        config: DefaultSvcConfig {
            mongo: None,
            sql: Some(sql_config.clone()),
        },
        mongo_client: None,
        sql_client: None,
    };

    svc.setup(&["sql"]).unwrap();
}
