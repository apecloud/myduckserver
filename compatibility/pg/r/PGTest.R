library(RPostgres)

Tests <- setRefClass(
  "Tests",
  fields = list(
    conn = "ANY",
    tests = "list"
  ),
  methods = list(
    connect = function(ip, port, user, password) {
      conn <<- dbConnect(
        Postgres(),
        host = ip,
        port = port,
        user = user,
        password = password,
        dbname = "postgres"
      )
    },
    disconnect = function() {
      dbDisconnect(conn)
    },
    addTest = function(query, expectedResults) {
      tests <<- c(tests, list(Test$new(query, expectedResults)))
    },
    runTests = function() {
      for (test in tests) {
        if (!test$run(conn)) {
          return(FALSE)
        }
      }
      return(TRUE)
    },
    readTestsFromFile = function(filename) {
      lines <- readLines(filename)
      i <- 1
      while (i <= length(lines)) {
        if (trimws(lines[i]) == "") {
          i <- i + 1
          next
        }
        query <- lines[i]
        i <- i + 1
        results <- list()
        while (i <= length(lines) && trimws(lines[i]) != "") {
          results <- c(results, list(strsplit(lines[i], ",")[[1]]))
          i <- i + 1
        }
        addTest(query, results)
      }
    }
  )
)

Test <- setRefClass(
  "Test",
  fields = list(
    query = "character",
    expectedResults = "list"
  ),
  methods = list(
    initialize = function(query, expectedResults) {
      query <<- query
      expectedResults <<- expectedResults
    },
    run = function(conn) {
      cat("Running test:", query, "\n")
      res <- dbSendQuery(conn, query)
      fetched <- dbFetch(res)
      dbClearResult(res)
      if (length(expectedResults) == 0) {
        if (nrow(fetched) == 0 || ncol(fetched) == 0) {
          return(TRUE)
        } else {
          cat("Expected empty result, got", nrow(fetched), "rows and", ncol(fetched), "columns\n")
          return(FALSE)
        }
      }
      if (ncol(fetched) != length(expectedResults[[1]])) {
        cat("Expected", length(expectedResults[[1]]), "columns, got", ncol(fetched), "\n")
        return(FALSE)
      }
      if (nrow(fetched) != length(expectedResults)) {
        cat("Expected", length(expectedResults), "rows, got", nrow(fetched), "\n")
        return(FALSE)
      }
      for (i in seq_len(nrow(fetched))) {
        for (j in seq_len(ncol(fetched))) {
          if (as.character(fetched[i, j]) != expectedResults[[i]][j]) {
            cat("Expected:", expectedResults[[i]][j], "Got:", fetched[i, j], "\n")
            return(FALSE)
          }
        }
      }
      return(TRUE)
    }
  )
)

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 5) {
  cat("Usage: Rscript PGTest.R <ip> <port> <user> <password> <testFile>\n")
  quit(status = 1)
}

tests <- Tests$new()
tests$connect(args[1], as.integer(args[2]), args[3], args[4])
tests$readTestsFromFile(args[5])

if (!tests$runTests()) {
  tests$disconnect()
  quit(status = 1)
}
tests$disconnect()