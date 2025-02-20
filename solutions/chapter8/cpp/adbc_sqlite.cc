// MIT License
//
// Copyright (c) 2021 Packt
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#include <iostream>
#include "helpers.h"

int main(int argc, char** argv) {
  AdbcError error;

  struct AdbcDatabase database = {};
  AbortNotOk(AdbcDatabaseNew(&database, &error), &error);
  AbortNotOk(AdbcDatabaseSetOption(&database, "uri", "file:data.db", &error), &error);
  AbortNotOk(AdbcDatabaseInit(&database, &error), &error);

  AdbcConnection conn;
  AdbcConnectionNew(&conn, nullptr);
  AbortNotOk(AdbcConnectionSetOption(&conn, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                     ADBC_OPTION_VALUE_ENABLED, &error),
             &error);
  AbortNotOk(AdbcConnectionInit(&conn, &database, &error), &error);

  AdbcStatement stmt;
  AdbcStatementNew(&conn, &stmt, nullptr);
  AbortNotOk(AdbcStatementSetSqlQuery(
                 &stmt, "CREATE TABLE IF NOT EXISTS foo ( col varchar(80) )", &error),
             &error);
  AbortNotOk(AdbcStatementExecuteQuery(&stmt, nullptr, nullptr, &error), &error);

  AbortNotOk(AdbcStatementSetSqlQuery(&stmt, "INSERT INTO foo VALUES ('bar')", &error),
             &error);

  int64_t rows_affected = 0;
  AbortNotOk(AdbcStatementExecuteQuery(&stmt, nullptr, &rows_affected, &error), &error);

  std::cout << "Rows Inserted: " << rows_affected << std::endl;

  Release(&stmt);
  Release(&conn);
  Release(&database);
}
