# Model Context Protocol

The Model Context Protocol (MCP) is an open protocol initiated by [Anthropic] that enables integration between LLM applications and external data sources and tools. MyDuck Server work seamlessly with the official PostgreSQL MCP server implementation. This guide will walk you through the process of setting up a MyDuck Server instance and connecting it to the PostgreSQL MCP server.

## Steps

1. Install and login to [Claude for Desktop](https://claude.ai/download).

2. Launch a new MyDuck Server instance. This can be done by using Docker or by [running the MyDuck Server binary](../../CONTRIBUTING.md). For this guide, we will use Docker.
    ```sh
    docker run -d -p 15432:5432 --rm -d --name myduck-server-for-mcp apecloud/myduckserver:latest
    ```

3. Insert some data into MyDuck Server. You can do this by connecting to MyDuck Server using a PostgreSQL client and running some SQL commands. For this guide, we will use the `psql` command-line client and the Data on Energy dataset from [Our World in Data](https://ourworldindata.org/energy).
    ```sh
    psql -h localhost -p 15432 -U postgres <<EOSQL
    CREATE TABLE energy AS FROM 'https://nyc3.digitaloceanspaces.com/owid-public/data/energy/owid-energy-data.csv';
    EOSQL
    ```

4. Add the following configuration to the "mcpServers" section of your Claude Desktop configuration file. This file can be found by clicking `Settings -> Developer -> Edit Config`.
    ```json
    {
        "mcpServers": {
            "postgres": {
                "command": "npx",
                "args": [
                    "-y",
                    "@modelcontextprotocol/server-postgres",
                    "postgresql://localhost:15432/postgres"
                ]
            }
        }
    }
    ```
    This configuration tells Claude Desktop to run the official PostgreSQL MCP server implementation and connect it to the MyDuck Server instance.

5. Restart Claude Desktop. You should see a new "tool" icon in the bottom right corner of the input box.

6. Ask Claude some questions about the data. For example, when we asked Claude: **“Which countries are the largest consumers of renewable energy in recent years?”**, here’s what we got: 

    [Screenshot](../../img/claude.png)

    > To answer this question, I'll use a SQL query to analyze renewable energy consumption data.
    > ```
    > {
    > `sql`: `
    > SELECT country, 
    >     ROUND(SUM(renewable_energy_consumption), 2) AS total_renewable_consumption,
    >     ROUND(AVG(renewable_energy_consumption), 2) AS avg_annual_consumption
    > FROM (
    >     SELECT country, 
    >         renewable_energy_consumption, 
    >         year
    >     FROM global_energy_data
    >     WHERE year >= 2018  -- Focus on recent years
    >     AND renewable_energy_consumption IS NOT NULL
    > ) recent_data
    > GROUP BY country
    > ORDER BY total_renewable_consumption DESC
    > LIMIT 10;
    > `
    > }
    > 
    > Error executing code: MCP error -32603: fallback query execution failed: Catalog Error: Table with name global_energy_data does not exist!
    > Did you mean "energy"?
    > LINE 9:     FROM global_energy_data
    >                 ^ (errno 1105) (sqlstate HY000)
    > ```
    > I apologize for the error. Let me try again with a corrected table name:
    > ```
    > {
    > `sql`: `
    > SELECT country, 
    >     ROUND(SUM(renewable_energy_consumption), 2) AS total_renewable_consumption,
    >     ROUND(AVG(renewable_energy_consumption), 2) AS avg_annual_consumption
    > FROM (
    >     SELECT country, 
    >         renewable_energy_consumption, 
    >         year
    >     FROM energy
    >     WHERE year >= 2018  -- Focus on recent years
    >     AND renewable_energy_consumption IS NOT NULL
    > ) recent_data
    > GROUP BY country
    > ORDER BY total_renewable_consumption DESC
    > LIMIT 10;
    > `
    > }
    > Error executing code: MCP error -32603: fallback query execution failed: Binder Error: Referenced column "renewable_energy_consumption" not found in FROM clause!
    > Candidate bindings: "energy.renewables_consumption", "energy.renewables_energy_per_capita", "energy.renewables_cons_change_pct", "energy.renewables_electricity", "energy.renewables_share_energy"
    > LINE 11:     AND renewable_energy_consumption IS NOT NULL
    > ) recent_data
    > GROUP BY country
    > ORDER BY total_renewable_consumption DESC
    > LIMIT 10;
    > ...
    >                 ^ (errno 1105) (sqlstate HY000)
    > ```
    > I'll modify the query to use the correct column name:
    > ```
    > {
    > `sql`: `
    > SELECT country, 
    >     ROUND(SUM(renewables_consumption), 2) AS total_renewable_consumption,
    >     ROUND(AVG(renewables_consumption), 2) AS avg_annual_consumption
    > FROM (
    >     SELECT country, 
    >         renewables_consumption, 
    >         year
    >     FROM energy
    >     WHERE year >= 2018  -- Focus on recent years
    >     AND renewables_consumption IS NOT NULL
    > ) recent_data
    > GROUP BY country
    > ORDER BY total_renewable_consumption DESC
    > LIMIT 10;
    > `
    > }
    > [
    > {
    >     "country": "World",
    >     "total_renewable_consumption": 131284.16,
    >     "avg_annual_consumption": 21880.69
    > },
    > {
    >     "country": "Non-OECD (EI)",
    >     "total_renewable_consumption": 74044.9,
    >     "avg_annual_consumption": 12340.82
    > },
    > {
    >     "country": "Upper-middle-income countries",
    >     "total_renewable_consumption": 61167.83,
    >     "avg_annual_consumption": 10194.64
    > },
    > {
    >     "country": "Asia",
    >     "total_renewable_consumption": 57425.35,
    >     "avg_annual_consumption": 9570.89
    > },
    > {
    >     "country": "OECD (EI)",
    >     "total_renewable_consumption": 57239.26,
    >     "avg_annual_consumption": 9539.88
    > },
    > {
    >     "country": "Asia Pacific (EI)",
    >     "total_renewable_consumption": 55581.96,
    >     "avg_annual_consumption": 9263.66
    > },
    > {
    >     "country": "High-income countries",
    >     "total_renewable_consumption": 53930.39,
    >     "avg_annual_consumption": 8988.4
    > },
    > {
    >     "country": "China",
    >     "total_renewable_consumption": 37488.51,
    >     "avg_annual_consumption": 6248.09
    > },
    > {
    >     "country": "Europe",
    >     "total_renewable_consumption": 28007.34,
    >     "avg_annual_consumption": 4667.89
    > },
    > {
    >     "country": "Europe (EI)",
    >     "total_renewable_consumption": 26720.13,
    >     "avg_annual_consumption": 4453.36
    > }
    > ]
    > ```
    > Let me refine the query to focus on specific countries:
    > ```
    > {
    > `sql`: `
    > SELECT country, 
    >     ROUND(SUM(renewables_consumption), 2) AS total_renewable_consumption,
    >     ROUND(AVG(renewables_consumption), 2) AS avg_annual_consumption
    > FROM (
    >     SELECT country, 
    >         renewables_consumption, 
    >         year
    >     FROM energy
    >     WHERE year >= 2018  -- Focus on recent years
    >     AND country NOT IN ('World', 'Asia', 'Europe', 'OECD (EI)', 'Non-OECD (EI)')
    >     AND renewables_consumption IS NOT NULL
    > ) recent_data
    > GROUP BY country
    > ORDER BY total_renewable_consumption DESC
    > LIMIT 10;
    > `
    > }
    > [
    > {
    >     "country": "Upper-middle-income countries",
    >     "total_renewable_consumption": 61167.83,
    >     "avg_annual_consumption": 10194.64
    > },
    > {
    >     "country": "Asia Pacific (EI)",
    >     "total_renewable_consumption": 55581.96,
    >     "avg_annual_consumption": 9263.66
    > },
    > {
    >     "country": "High-income countries",
    >     "total_renewable_consumption": 53930.39,
    >     "avg_annual_consumption": 8988.4
    > },
    > {
    >     "country": "China",
    >     "total_renewable_consumption": 37488.51,
    >     "avg_annual_consumption": 6248.09
    > },
    > {
    >     "country": "Europe (EI)",
    >     "total_renewable_consumption": 26720.13,
    >     "avg_annual_consumption": 4453.36
    > },
    > {
    >     "country": "North America",
    >     "total_renewable_consumption": 24530.2,
    >     "avg_annual_consumption": 4088.37
    > },
    > {
    >     "country": "North America (EI)",
    >     "total_renewable_consumption": 24399.44,
    >     "avg_annual_consumption": 4066.57
    > },
    > {
    >     "country": "European Union (27)",
    >     "total_renewable_consumption": 18299.66,
    >     "avg_annual_consumption": 3049.94
    > },
    > {
    >     "country": "South and Central America (EI)",
    >     "total_renewable_consumption": 16656,
    >     "avg_annual_consumption": 2776
    > },
    > {
    >     "country": "United States",
    >     "total_renewable_consumption": 16236.34,
    >     "avg_annual_consumption": 2706.06
    > }
    > ]
    > ```
    > Based on the data, here are the key insights about renewable energy consumption in recent years:
    > 
    > 1. China stands out as the largest individual country consumer of renewable energy, with an average annual consumption of 6,248.09 units and a total of 37,488.51 units from 2018 onwards.
    > 
    > 2. The United States is also a significant consumer, with an average annual consumption of 2,706.06 units and a total of 16,236.34 units.
    > 
    > 3. Regionally, some notable observations:
    > - Upper-middle-income countries lead in renewable energy consumption
    > - Asia Pacific region shows strong renewable energy adoption
    > - The European Union (27 countries) collectively consumes a significant amount of renewable energy
    > 
    > However, the data seems to mix country-level and regional/income-group data, which makes precise country-level comparisons challenging. To get a clearer picture, I'll try one more query:
    > ```
    > {
    > `sql`: `
    > SELECT country, 
    >     ROUND(AVG(renewables_cons_change_pct), 2) AS avg_growth_rate,
    >     ROUND(AVG(renewables_share_energy), 2) AS avg_renewables_share
    > FROM energy
    > WHERE year >= 2018 
    > AND country NOT IN ('World', 'Asia', 'Europe', 'OECD (EI)', 'Non-OECD (EI)')
    > AND country NOT LIKE '%income countries%'
    > AND country NOT LIKE '%America%'
    > GROUP BY country
    > ORDER BY avg_renewables_share DESC
    > LIMIT 15;
    > `
    > }
    > [
    > {
    >     "country": "Norway",
    >     "avg_growth_rate": 0.96,
    >     "avg_renewables_share": 70.76
    > },
    > {
    >     "country": "Sweden",
    >     "avg_growth_rate": 3.92,
    >     "avg_renewables_share": 49.69
    > },
    > {
    >     "country": "Brazil",
    >     "avg_growth_rate": 5.04,
    >     "avg_renewables_share": 47.5
    > },
    > {
    >     "country": "New Zealand",
    >     "avg_growth_rate": 1.28,
    >     "avg_renewables_share": 39.92
    > },
    > {
    >     "country": "Denmark",
    >     "avg_growth_rate": 4.92,
    >     "avg_renewables_share": 37.87
    > },
    > {
    >     "country": "Austria",
    >     "avg_growth_rate": 1.54,
    >     "avg_renewables_share": 36.67
    > },
    > {
    >     "country": "Switzerland",
    >     "avg_growth_rate": 3.83,
    >     "avg_renewables_share": 35.34
    > },
    > {
    >     "country": "Eastern Africa (EI)",
    >     "avg_growth_rate": 4.68,
    >     "avg_renewables_share": 33.89
    > },
    > {
    >     "country": "Finland",
    >     "avg_growth_rate": 4.53,
    >     "avg_renewables_share": 33.17
    > },
    > {
    >     "country": "Ecuador",
    >     "avg_growth_rate": 4.1,
    >     "avg_renewables_share": 31.01
    > },
    > {
    >     "country": "Portugal",
    >     "avg_growth_rate": 7.95,
    >     "avg_renewables_share": 30.65
    > },
    > {
    >     "country": "Colombia",
    >     "avg_growth_rate": 2.64,
    >     "avg_renewables_share": 29.71
    > },
    > {
    >     "country": "Canada",
    >     "avg_growth_rate": -0.3,
    >     "avg_renewables_share": 29.7
    > },
    > {
    >     "country": "Peru",
    >     "avg_growth_rate": 0.93,
    >     "avg_renewables_share": 29.03
    > },
    > {
    >     "country": "Middle Africa (EI)",
    >     "avg_growth_rate": 5.52,
    >     "avg_renewables_share": 28.98
    > }
    > ]
    > ```
    > 
    > This provides a more nuanced view of renewable energy consumption. Key findings include:
    > 
    > 1. Norway leads with the highest share of renewable energy at 70.76% of its total energy consumption.
    > 2. Sweden follows with 49.69% renewable energy share and a strong growth rate of 3.92%.
    > 3. Brazil is notable with a 47.5% renewable energy share and an impressive 5.04% growth rate.
    > 4. Several European countries (Denmark, Austria, Switzerland, Finland, Portugal) feature prominently in renewable energy adoption.
    > 5. Some countries like New Zealand and Canada also have significant renewable energy shares.
    > 
    > In summary, while China and the United States are the largest consumers in absolute terms, countries like Norway, Sweden, and Brazil are leading in terms of renewable energy share and growth rates. The transition to renewable energy is a global trend, with different countries showing strength in various aspects of renewable energy consumption.