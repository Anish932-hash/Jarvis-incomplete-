'use client';

import { Avatar, AvatarFallback } from '@/components/ui/avatar';
import { Bot, User } from 'lucide-react';
import { cn } from '@/lib/utils';
import Image from 'next/image';

export interface Message {
  role: 'user' | 'model';
  content: string;
  imageUrl?: string;
  id: string;
}

const ChatMessage = ({ message }: { message: Message }) => {
  const isUser = message.role === 'user';
  const isThinking = message.role === 'model' && message.content === '' && !message.imageUrl;

  return (
    <div
      className={cn(
        'flex items-start gap-4 animate-fade-in',
        isUser ? 'justify-end' : 'justify-start'
      )}
    >
      {!isUser && (
        <Avatar className="h-8 w-8 border border-primary/50 shadow-md shadow-primary/20">
          <AvatarFallback className="bg-background text-primary">
            <Bot size={18} />
          </AvatarFallback>
        </Avatar>
      )}

      <div
        className={cn(
          'max-w-md rounded-lg p-3 lg:max-w-xl',
          isUser
            ? 'rounded-br-none bg-primary/20'
            : 'rounded-bl-none bg-muted/50'
        )}
      >
        {isThinking ? (
             <div className="flex items-center gap-2 text-primary/80 p-1">
                <div className="h-2 w-2 animate-pulse rounded-full bg-primary [animation-delay:-0.3s]" />
                <div className="h-2 w-2 animate-pulse rounded-full bg-primary [animation-delay:-0.15s]" />
                <div className="h-2 w-2 animate-pulse rounded-full bg-primary" />
            </div>
        ) : (
            <p className="text-foreground/90 whitespace-pre-wrap">{message.content}</p>
        )}
        {message.imageUrl && (
            <div className="relative mt-2 h-48 w-48 overflow-hidden rounded-md border border-primary/20">
                <Image src={message.imageUrl} alt="Uploaded" fill className="object-cover" sizes="192px" unoptimized />
            </div>
        )}
      </div>

      {isUser && (
        <Avatar className="h-8 w-8 border border-accent/50 shadow-md shadow-accent/20">
          <AvatarFallback className="bg-background text-accent">
            <User size={18} />
          </AvatarFallback>
        </Avatar>
      )}
    </div>
  );
};

export default ChatMessage;
