import subprocess

class PokerEvaluatorBridge:
    def __init__(self, executable_path="./evaluator/poker_checker"):
        # 啟動 C++ 程式，開啟 --machine 模式以移除提示字元
        self.process = subprocess.Popen(
            [executable_path, "--machine"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1  # 行緩衝
        )

    def judge(self, player_a, player_b, community):
        """
        傳送資料並接收結果
        player_a: ["AS", "KS"]
        player_b: ["QH", "QD"]
        community: ["2D", "QS", "7C", "9S", "JS"]
        """
        # 組合符合 C++ 格式的字串: "AS KS | QH QD | 2D QS 7C 9S JS"
        input_str = f"{' '.join(player_a)} | {' '.join(player_b)} | {' '.join(community)}\n"
        
        # 送出指令
        self.process.stdin.write(input_str)
        self.process.stdin.flush()
        
        # 讀取並解析 C++ 的三行輸出
        best_a = self.process.stdout.readline().strip().replace("A's Best: ", "")
        best_b = self.process.stdout.readline().strip().replace("B's Best: ", "")
        winner_text = self.process.stdout.readline().strip().replace("Result: ", "")
        self.process.stdout.readline() # 消耗空行
        
        return {
            "playerA_best": best_a,
            "playerB_best": best_b,
            "winner": winner_text
        }

    def close(self):
        self.process.stdin.write("exit\n")
        self.process.stdin.flush()
        self.process.terminate()

# 測試範例
if __name__ == "__main__":
    evaluator = PokerEvaluatorBridge()
    
    # 範例對局
    scenarios = [
        (["AS", "KS"], ["QH", "QD"], ["2D", "QS", "7C", "10S", "JS"]),
        (["AH", "2C"], ["KD", "QH"], ["10S", "JC", "QD", "KH", "9S"])
    ]
    
    for a, b, comm in scenarios:
        results = evaluator.judge(a, b, comm)
        print(f"--- 測試結果 ---")
        print(f"輸入: {' '.join(a)} | {' '.join(b)} | {' '.join(comm)}")
        print(f"A 牌型: {results['playerA_best']}")
        print(f"B 牌型: {results['playerB_best']}")
        print(f"勝負: {results['winner']}\n")
    
    evaluator.close()
