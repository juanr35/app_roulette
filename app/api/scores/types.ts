export interface RouletteOutcome {
  number: number
  type: string
  color: string
}

export interface RouletteTable {
  id: string
  name: string
}

export interface RouletteData {
  id: string
  startedAt: Date
  settledAt: Date
  status: string
  gameType: string
  table: RouletteTable
  result: {
    outcome: RouletteOutcome
    luckyNumbersList?: Array<{
      number: number
      roundedMultiplier: number
    }>
  }
}

export interface RouletteEvent {
  id: string
  data: RouletteData
}