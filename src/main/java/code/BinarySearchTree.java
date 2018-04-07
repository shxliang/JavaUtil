package code;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @author lsx
 * @date 2018/4/6
 */
public class BinarySearchTree {

    public static class BST {
        int value;
        BST left;
        BST right;

        BST(int value) {
            this.value = value;
            this.left = null;
            this.right = null;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public BST getLeft() {
            return left;
        }

        public void setLeft(BST left) {
            this.left = left;
        }

        public BST getRight() {
            return right;
        }

        public void setRight(BST right) {
            this.right = right;
        }
    }

    public static BST init() {
        BST root = new BST(5);
        BST curNode = root;
        for (int i = 0; i < 5; i++) {
            curNode.setLeft(new BST(curNode.getValue() - 1));
            curNode.setRight(new BST(curNode.getValue() + 1));
            curNode = curNode.getLeft();
        }
        return root;
    }


    public static void search(BST root, int value) {
        if (root == null)
        {
            return;
        }
        System.out.println(root.getValue());
        if (root.getValue() == value) {
            System.out.println("find!");
            return;
        } else if (value < root.getValue()) {
            search(root.getLeft(), value);
        } else {
            search(root.getRight(), value);
        }
    }

    public static void preOrder(BST root)
    {
        if (root == null)
        {
            return;
        }
        System.out.println(root.getValue());
        preOrder(root.getLeft());
        preOrder(root.getRight());
    }

    public static void layerOrder(BST root)
    {
        Queue<BST> queue = new LinkedBlockingDeque<>();
        queue.add(root);
        while(queue.size() > 0)
        {
            BST curNode = queue.poll();
            System.out.println(curNode.getValue());
            if(curNode.getLeft() != null)
            {
                queue.add(curNode.getLeft());
            }
            if(curNode.getRight() != null)
            {
                queue.add(curNode.getRight());
            }
        }
    }

    public static void main(String[] args) {
        System.out.println(1/2);
    }
}
