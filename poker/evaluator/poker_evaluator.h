#ifndef POKER_EVALUATOR_H
#define POKER_EVALUATOR_H

#include <vector>
#include <string>
#include <algorithm>
#include <map>

enum class Suit {
    Spades = 4,
    Hearts = 3,
    Diamonds = 2,
    Clubs = 1
};

enum class Rank {
    Two = 2, Three, Four, Five, Six, Seven, Eight, Nine, Ten,
    Jack, Queen, King, Ace
};

enum class HandRank {
    HighCard = 1,
    OnePair,
    TwoPair,
    ThreeOfAKind,
    Straight,
    Flush,
    FullHouse,
    FourOfAKind,
    StraightFlush,
    RoyalFlush
};

struct Card {
    Suit suit;
    Rank rank;

    bool operator<(const Card& other) const {
        return rank < other.rank;
    }
};

struct EvalResult {
    HandRank handRank;
    std::vector<int> values; // 用於比牌的權重值（如：三條的點數，接下來是踢腳牌）

    bool operator>(const EvalResult& other) const {
        if (handRank != other.handRank)
            return handRank > other.handRank;
        return values > other.values;
    }

    bool operator==(const EvalResult& other) const {
        return handRank == other.handRank && values == other.values;
    }
};

class PokerEvaluator {
public:
    static EvalResult evaluateHand(std::vector<Card> fiveCards);
    static EvalResult getBestHand(const std::vector<Card>& hand, const std::vector<Card>& community);
    static int judge(const std::vector<Card>& playerA, const std::vector<Card>& playerB, const std::vector<Card>& community);
    
    static std::string handRankToString(HandRank rank);
    static Card parseCard(const std::string& str);
    static std::string cardToString(const Card& card);
};

#endif
