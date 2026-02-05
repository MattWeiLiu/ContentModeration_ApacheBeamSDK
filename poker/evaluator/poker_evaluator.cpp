#include "poker_evaluator.h"
#include <map>
#include <set>

EvalResult PokerEvaluator::evaluateHand(std::vector<Card> cards) {
    std::sort(cards.begin(), cards.end(), [](const Card& a, const Card& b) {
        return (int)a.rank > (int)b.rank;
    });

    bool flush = true;
    for (size_t i = 1; i < cards.size(); ++i) {
        if (cards[i].suit != cards[0].suit) {
            flush = false;
            break;
        }
    }

    bool straight = true;
    for (size_t i = 1; i < cards.size(); ++i) {
        if ((int)cards[i-1].rank != (int)cards[i].rank + 1) {
            straight = false;
            break;
        }
    }

    // Special case for A, 2, 3, 4, 5 straight
    if (!straight && cards[0].rank == Rank::Ace && cards[1].rank == Rank::Five &&
        cards[2].rank == Rank::Four && cards[3].rank == Rank::Three && cards[4].rank == Rank::Two) {
        straight = true;
        // Re-order to put Ace at the end for value purposes
        std::rotate(cards.begin(), cards.begin() + 1, cards.end());
    }

    std::map<Rank, int> counts;
    for (const auto& card : cards) counts[card.rank]++;

    std::vector<std::pair<int, Rank>> freq;
    for (auto const& [rank, count] : counts) freq.push_back({count, rank});
    std::sort(freq.rbegin(), freq.rend());

    EvalResult res;
    if (straight && flush) {
        res.handRank = (cards[0].rank == Rank::Ace && cards[1].rank == Rank::King) ? HandRank::RoyalFlush : HandRank::StraightFlush;
        for (const auto& c : cards) res.values.push_back((int)c.rank);
    } else if (freq[0].first == 4) {
        res.handRank = HandRank::FourOfAKind;
        res.values = {(int)freq[0].second, (int)freq[1].second};
    } else if (freq[0].first == 3 && freq[1].first == 2) {
        res.handRank = HandRank::FullHouse;
        res.values = {(int)freq[0].second, (int)freq[1].second};
    } else if (flush) {
        res.handRank = HandRank::Flush;
        for (const auto& c : cards) res.values.push_back((int)c.rank);
    } else if (straight) {
        res.handRank = HandRank::Straight;
        for (const auto& c : cards) res.values.push_back((int)c.rank);
    } else if (freq[0].first == 3) {
        res.handRank = HandRank::ThreeOfAKind;
        res.values = {(int)freq[0].second, (int)freq[1].second, (int)freq[2].second};
    } else if (freq[0].first == 2 && freq[1].first == 2) {
        res.handRank = HandRank::TwoPair;
        res.values = {(int)freq[0].second, (int)freq[1].second, (int)freq[2].second};
    } else if (freq[0].first == 2) {
        res.handRank = HandRank::OnePair;
        res.values = {(int)freq[0].second, (int)freq[1].second, (int)freq[2].second, (int)freq[3].second};
    } else {
        res.handRank = HandRank::HighCard;
        for (const auto& c : cards) res.values.push_back((int)c.rank);
    }
    return res;
}

EvalResult PokerEvaluator::getBestHand(const std::vector<Card>& hand, const std::vector<Card>& community) {
    std::vector<Card> allCards = hand;
    allCards.insert(allCards.end(), community.begin(), community.end());

    EvalResult best;
    best.handRank = (HandRank)0;

    // Pick 5 out of 7
    std::vector<int> p = {0, 0, 1, 1, 1, 1, 1};
    do {
        std::vector<Card> combo;
        for (int i = 0; i < 7; ++i) {
            if (p[i]) combo.push_back(allCards[i]);
        }
        EvalResult current = evaluateHand(combo);
        if (best.handRank == (HandRank)0 || current > best) {
            best = current;
        }
    } while (std::next_permutation(p.begin(), p.end()));

    return best;
}

int PokerEvaluator::judge(const std::vector<Card>& playerA, const std::vector<Card>& playerB, const std::vector<Card>& community) {
    EvalResult resA = getBestHand(playerA, community);
    EvalResult resB = getBestHand(playerB, community);

    if (resA > resB) return 1;
    if (resB > resA) return -1;
    return 0;
}

std::string PokerEvaluator::handRankToString(HandRank rank) {
    switch (rank) {
        case HandRank::HighCard: return "High Card";
        case HandRank::OnePair: return "One Pair";
        case HandRank::TwoPair: return "Two Pair";
        case HandRank::ThreeOfAKind: return "Three of a Kind";
        case HandRank::Straight: return "Straight";
        case HandRank::Flush: return "Flush";
        case HandRank::FullHouse: return "Full House";
        case HandRank::FourOfAKind: return "Four of a Kind";
        case HandRank::StraightFlush: return "Straight Flush";
        case HandRank::RoyalFlush: return "Royal Flush";
        default: return "Unknown";
    }
}

Card PokerEvaluator::parseCard(const std::string& str) {
    if (str.length() < 2) return {Suit::Spades, Rank::Two};
    
    Suit s;
    char suitChar = toupper(str.back());
    if (suitChar == 'S') s = Suit::Spades;
    else if (suitChar == 'H') s = Suit::Hearts;
    else if (suitChar == 'D') s = Suit::Diamonds;
    else if (suitChar == 'C') s = Suit::Clubs;
    else s = Suit::Spades;

    std::string rankStr = str.substr(0, str.length() - 1);
    Rank r;
    if (rankStr == "A") r = Rank::Ace;
    else if (rankStr == "K") r = Rank::King;
    else if (rankStr == "Q") r = Rank::Queen;
    else if (rankStr == "J") r = Rank::Jack;
    else if (rankStr == "10") r = Rank::Ten;
    else {
        int val = rankStr[0] - '0';
        r = (Rank)val;
    }
    return {s, r};
}

std::string PokerEvaluator::cardToString(const Card& c) {
    std::string s;
    switch (c.suit) {
        case Suit::Spades: s = "S"; break;
        case Suit::Hearts: s = "H"; break;
        case Suit::Diamonds: s = "D"; break;
        case Suit::Clubs: s = "C"; break;
    }
    int r = (int)c.rank;
    std::string rs;
    if (r <= 10) rs = std::to_string(r);
    else if (r == 11) rs = "J";
    else if (r == 12) rs = "Q";
    else if (r == 13) rs = "K";
    else if (r == 14) rs = "A";
    return rs + s;
}
