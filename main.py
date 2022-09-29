import WikiData

session = WikiData.WikiData()

df = session.get_continents_and_countries()
df = session.run_all_parallel()
print(df)

df.to_csv('results.csv')
