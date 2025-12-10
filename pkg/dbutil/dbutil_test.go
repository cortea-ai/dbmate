package dbutil_test

import (
	"database/sql"
	"testing"

	"github.com/amacneil/dbmate/v2/pkg/dbtest"
	"github.com/amacneil/dbmate/v2/pkg/dbutil"

	_ "github.com/mattn/go-sqlite3" // database/sql driver
	"github.com/stretchr/testify/require"
)

func TestDatabaseName(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		u := dbtest.MustParseURL(t, "foo://host/dbname?query")
		name := dbutil.DatabaseName(u)
		require.Equal(t, "dbname", name)
	})

	t.Run("empty", func(t *testing.T) {
		u := dbtest.MustParseURL(t, "foo://host")
		name := dbutil.DatabaseName(u)
		require.Equal(t, "", name)
	})
}

func TestTrimLeadingSQLComments(t *testing.T) {
	t.Run("leading comments", func(t *testing.T) {
		in := "--\n" +
			"-- foo\n\n" +
			"-- bar\n\n" +
			"real stuff\n" +
			"-- end\n"
		out, err := dbutil.TrimLeadingSQLComments([]byte(in))
		require.NoError(t, err)
		require.Equal(t, "real stuff\n-- end\n", string(out))
	})

	t.Run("restrict/unrestrict meta-commands", func(t *testing.T) {
		in := `\restrict abc123
-- comment
real stuff
\unrestrict abc123
`
		out, err := dbutil.TrimLeadingSQLComments([]byte(in))
		require.NoError(t, err)
		require.Equal(t, "real stuff\n", string(out))
	})

	t.Run("empty search_path fix for sqlc", func(t *testing.T) {
		in := `-- comment
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
`
		out, err := dbutil.TrimLeadingSQLComments([]byte(in))
		require.NoError(t, err)
		require.Equal(t, "SELECT pg_catalog.set_config('search_path', 'public', false);\nSET check_function_bodies = false;\n", string(out))
	})

	t.Run("strip public schema prefix for sqlc", func(t *testing.T) {
		in := `-- comment
SELECT pg_catalog.set_config('search_path', '', false);
CREATE TYPE public.language AS ENUM ('ENGLISH', 'GERMAN');
CREATE TABLE public.projects (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    language public.language NOT NULL
);
`
		out, err := dbutil.TrimLeadingSQLComments([]byte(in))
		require.NoError(t, err)
		expected := `SELECT pg_catalog.set_config('search_path', 'public', false);
CREATE TYPE language AS ENUM ('ENGLISH', 'GERMAN');
CREATE TABLE projects (
    id uuid DEFAULT uuid_generate_v4() NOT NULL,
    language language NOT NULL
);
`
		require.Equal(t, expected, string(out))
	})

	t.Run("preserve public. inside string literals", func(t *testing.T) {
		in := `-- comment
SELECT pg_catalog.set_config('search_path', '', false);
CREATE TABLE public.config (
    url text DEFAULT 'https://public.example.com' NOT NULL,
    description text DEFAULT 'public.schema' NOT NULL
);
`
		out, err := dbutil.TrimLeadingSQLComments([]byte(in))
		require.NoError(t, err)
		expected := `SELECT pg_catalog.set_config('search_path', 'public', false);
CREATE TABLE config (
    url text DEFAULT 'https://public.example.com' NOT NULL,
    description text DEFAULT 'public.schema' NOT NULL
);
`
		require.Equal(t, expected, string(out))
	})
}

// connect to in-memory sqlite database for testing
const sqliteMemoryDB = "file:dbutil.sqlite3?mode=memory&cache=shared"

func TestQueryColumn(t *testing.T) {
	db, err := sql.Open("sqlite3", sqliteMemoryDB)
	require.NoError(t, err)

	val, err := dbutil.QueryColumn(db, "select 'foo_' || val from (select ? as val union select ?)",
		"hi", "there")
	require.NoError(t, err)
	require.Equal(t, []string{"foo_hi", "foo_there"}, val)
}

func TestQueryValue(t *testing.T) {
	db, err := sql.Open("sqlite3", sqliteMemoryDB)
	require.NoError(t, err)

	val, err := dbutil.QueryValue(db, "select $1 + $2", "5", 2)
	require.NoError(t, err)
	require.Equal(t, "7", val)
}
