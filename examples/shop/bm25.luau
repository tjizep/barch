--!strict

export type BM25Config = {
	k1: number,
	b: number
}

type TokenizedDocument = {
	tokens: {string},
	frequencies: {[string]: number},
	length: number
}

type BM25Instance = {
	k1: number,
	b: number,
	corpusSize: number,
	avgDocLength: number,
	idf: {[string]: number},
	documents: {TokenizedDocument},
	score: (self: BM25Instance, query: string) -> { {index: number, score: number} }
}

local BM25 = {}
BM25.__index = BM25

-- Helper to safely tokenize and lowercase raw text strings
local function tokenize(text: string): {string}
	local tokens: {string} = {}
	for word in string.gmatch(string.lower(text), "%w+") do
		table.insert(tokens, word)
	end
	return tokens
end

-- Constructor function
function BM25.new(corpus: {string}, config: BM25Config?): BM25Instance
	local self = setmetatable({}, BM25) :: any

	-- Set hyper-parameters (standard defaults)
	self.k1 = config and config.k1 or 1.5
	self.b = config and config.b or 0.75

	self.corpusSize = #corpus
	self.documents = table.create(self.corpusSize) :: {TokenizedDocument}
	self.idf = {} :: {[string]: number}

	local totalLength = 0
	local docFrequencies: {[string]: number} = {}

	-- Step 1: Pre-process documents and count frequencies
	for i, rawDoc in ipairs(corpus) do
		local tokens = tokenize(rawDoc)
		local docLength = #tokens
		totalLength += docLength

		local frequencies: {[string]: number} = {}
		local uniqueSeen: {[string]: boolean} = {}

		for _, token in ipairs(tokens) do
			frequencies[token] = (frequencies[token] or 0) + 1

			if not uniqueSeen[token] then
				uniqueSeen[token] = true
				docFrequencies[token] = (docFrequencies[token] or 0) + 1
			end
		end

		self.documents[i] = {
			tokens = tokens,
			frequencies = frequencies,
			length = docLength
		}
	end

	self.avgDocLength = self.corpusSize > 0 and (totalLength / self.corpusSize) or 0

	-- Step 2: Compute IDF using the standard Lucene variant
	for token, docCount in pairs(docFrequencies) do
	-- Adds 0.5 to prevent negative values for very common terms
		self.idf[token] = math.log((self.corpusSize - docCount + 0.5) / (docCount + 0.5) + 1)
	end

	return self
end

-- Ranking function matching queries to corpus documents
function BM25:score(query: string): { {index: number, score: number} }
	local queryTokens = tokenize(query)
	local results: { {index: number, score: number} } = table.create(self.corpusSize)

	for docIndex, doc in ipairs(self.documents) do
		local totalScore = 0.0

		for _, token in ipairs(queryTokens) do
			local idf = self.idf[token]
			if not idf then continue end -- Term doesn't exist in corpus

			local tf = doc.frequencies[token] or 0
			if tf == 0 then continue end

			-- Core BM25 Formula
			local numerator = tf * (self.k1 + 1)
			local denominator = tf + self.k1 * (1 - self.b + self.b * (doc.length / self.avgDocLength))

			totalScore += idf * (numerator / denominator)
		end

		results[docIndex] = {
			index = docIndex,
			score = totalScore
		}
	end

	-- Sort array by descending relevance score
	table.sort(results, function(a, b)
		return a.score > b.score
	end)

	return results
end

return BM25
