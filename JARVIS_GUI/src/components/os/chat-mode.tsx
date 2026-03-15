'use client';

import LoopingVoiceHUD from './looping-voice-hud';
import ActionControlPanel from './action-control-panel';
import DesktopRecoveryBanner from './desktop-recovery-banner';
import ModelSetupRecoveryBanner from './model-setup-recovery-banner';
import { type AppMode } from '@/app/page';
import { Grid, MessageSquare, Send, Paperclip, Wrench, X } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { useState, useRef, useEffect } from 'react';
import type { Message } from './chat-message';
import ChatMessage from './chat-message';
import { nanoid } from 'nanoid';
import { ScrollArea } from '@/components/ui/scroll-area';
import { useToast } from '@/hooks/use-toast';
import { backendClient } from '@/lib/backend-client';
import Image from 'next/image';

const Corner = (props: React.HTMLAttributes<SVGElement>) => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    viewBox="0 0 24 24"
    fill="currentColor"
    {...props}
  >
    <path d="M0 0 H 12 V 4 H 4 V 12 H 0 Z" />
  </svg>
);


const ChatMode = ({ setMode }: { setMode: (mode: AppMode) => void }) => {
    const [messages, setMessages] = useState<Message[]>([]);
    const [input, setInput] = useState('');
    const [isLoading, setIsLoading] = useState(false);
    const [photoDataUri, setPhotoDataUri] = useState<string | null>(null);
    const [photoPreview, setPhotoPreview] = useState<string | null>(null);
    const { toast } = useToast();

    const fileInputRef = useRef<HTMLInputElement>(null);
    const scrollAreaRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        // Scroll to bottom when messages change
        if (scrollAreaRef.current) {
            const viewport = scrollAreaRef.current.querySelector('div[data-radix-scroll-area-viewport]');
            if (viewport) {
                viewport.scrollTop = viewport.scrollHeight;
            }
        }
      }, [messages, isLoading]);

    const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
        const file = event.target.files?.[0];
        if (file) {
             if (!file.type.startsWith('image/')) {
                alert('Only image files are supported.');
                return;
            }

            const reader = new FileReader();
            reader.onload = (e) => {
                const dataUri = e.target?.result as string;
                setPhotoDataUri(dataUri);
                setPhotoPreview(URL.createObjectURL(file));
            };
            reader.readAsDataURL(file);
        }
    };
    
    const handleRemoveImage = () => {
        setPhotoDataUri(null);
        setPhotoPreview(null);
        if(fileInputRef.current) {
            fileInputRef.current.value = '';
        }
    }

    const handleSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
        e.preventDefault();
        if ((!input.trim() && !photoDataUri) || isLoading) return;

        const userMessage: Message = {
            id: nanoid(),
            role: 'user',
            content: input,
            imageUrl: photoPreview ?? undefined,
        };

        const currentPhotoDataUri = photoDataUri;

        setMessages((prev) => [...prev, userMessage]);
        setIsLoading(true);
        setInput('');
        handleRemoveImage();

        const history = messages.map((msg) => ({
            role: msg.role,
            content: [{ text: msg.content }],
        }));

        try {
            const response = await backendClient.chat({
                history,
                message: userMessage.content,
                photoDataUri: currentPhotoDataUri ?? undefined,
            });

            const aiMessage: Message = {
                id: nanoid(),
                role: 'model',
                content: response.reply,
            };
            setMessages((prev) => [...prev, aiMessage]);
        } catch (error) {
            console.error("Error calling chat flow:", error);
            const errorMessage: Message = {
                id: nanoid(),
                role: 'model',
                content: "My apologies, I seem to be having some trouble connecting to my core processors. Please try again in a moment.",
            };
            setMessages((prev) => [...prev, errorMessage]);
        } finally {
            setIsLoading(false);
        }
      };

    return (
        <div className="relative flex h-screen w-screen flex-col items-center justify-center p-4 font-headline bg-background">
            {/* Corner decorations */}
            <Corner className="absolute top-4 left-4 h-8 w-8 text-primary/50" />
            <Corner className="absolute top-4 right-4 h-8 w-8 -scale-x-100 text-primary/50" />
            <Corner className="absolute bottom-4 left-4 h-8 w-8 -scale-y-100 text-primary/50" />
            <Corner className="absolute bottom-4 right-4 h-8 w-8 scale-x-[-1] scale-y-[-1] text-primary/50" />

            {/* Top Header */}
            <div className="absolute top-4 left-1/2 -translate-x-1/2 flex items-center gap-4 text-primary tracking-widest text-sm">
                <button onClick={() => setMode('hud')} className="rounded-md p-1 transition-all hover:bg-primary/10 hover:text-accent hover:shadow-[0_0_15px_hsl(var(--primary)/0.3)]" title="Switch to HUD Mode">
                    <Grid size={16} />
                </button>
                <button
                    className="flex items-center gap-2 rounded-md p-1 transition-all hover:bg-primary/10 hover:text-accent hover:shadow-[0_0_15px_hsl(var(--primary)/0.3)]"
                    onClick={() => {
                        toast({
                            title: "J.A.R.V.I.S. AI Chat",
                            description: "You are communicating with J.A.R.V.I.S. (Just A Rather Very Intelligent System).",
                        })
                    }}
                >
                    <MessageSquare size={16} />
                    <span>AI CHAT</span>
                </button>
                <ActionControlPanel
                    trigger={
                        <button
                            className="rounded-md p-1 transition-all hover:bg-primary/10 hover:text-accent hover:shadow-[0_0_15px_hsl(var(--primary)/0.3)]"
                            title="Open Action Panel"
                        >
                            <Wrench size={16} />
                        </button>
                    }
                />
            </div>

            <div className="flex w-full max-w-4xl flex-1 flex-col overflow-hidden pt-12 pb-4">
                <div className="mx-auto mb-3 flex w-full max-w-3xl shrink-0 flex-col gap-2">
                    <DesktopRecoveryBanner className="w-full" compact />
                    <ModelSetupRecoveryBanner className="w-full" compact />
                </div>
                <div className="flex min-h-0 flex-1 items-center justify-center overflow-hidden">
                    {messages.length === 0 && !isLoading ? (
                        <LoopingVoiceHUD />
                    ) : (
                    <ScrollArea className="h-full w-full" ref={scrollAreaRef}>
                        <div className="flex flex-col gap-6 p-4">
                        {messages.map((m) => (
                            <ChatMessage key={m.id} message={m} />
                        ))}
                        {isLoading && (
                            <ChatMessage message={{id: 'thinking', role: 'model', content: ''}} />
                        )}
                        </div>
                    </ScrollArea>
                    )}
                </div>
            </div>

            <div className="w-full max-w-2xl pb-8">
                <form onSubmit={handleSubmit} className="relative w-full max-w-2xl">
                    <div className="relative rounded-full border border-primary/20 bg-background/30 backdrop-blur-sm shadow-[0_0_15px_hsl(var(--primary)/0.1)] transition-all focus-within:border-primary/50 focus-within:shadow-[0_0_20px_hsl(var(--primary)/0.2)]">
                        {photoPreview && (
                            <div className="absolute bottom-16 left-4">
                                    <div className="relative h-20 w-20 rounded-md border border-primary/30 p-1 bg-background/50">
                                    <Image src={photoPreview} alt="Preview" fill className="rounded object-cover p-1" sizes="80px" unoptimized />
                                    <Button type="button" size="icon" variant="destructive" className="absolute -top-2 -right-2 h-6 w-6 rounded-full" onClick={handleRemoveImage}>
                                        <X size={14}/>
                                    </Button>
                                </div>
                            </div>
                        )}
                        <Input
                            value={input}
                            onChange={(e) => setInput(e.target.value)}
                            placeholder={isLoading ? "J.A.R.V.I.S. is thinking..." : "Message J.A.R.V.I.S...."}
                            className="h-12 w-full rounded-full border-none bg-transparent pr-28 pl-14 text-base focus-visible:ring-0"
                            disabled={isLoading}
                        />
                        <input type="file" ref={fileInputRef} onChange={handleFileChange} accept="image/*" className="hidden" />
                        
                        <Button type="button" size="icon" className="absolute left-2 top-1/2 -translate-y-1/2 h-9 w-9 rounded-full border border-primary/50 bg-transparent text-primary transition-all duration-300 hover:bg-primary hover:text-primary-foreground hover:shadow-[0_0_15px_hsl(var(--primary))]" onClick={() => fileInputRef.current?.click()} disabled={isLoading}>
                            <Paperclip size={18} />
                        </Button>

                        <Button type="submit" size="icon" className="absolute right-2 top-1/2 -translate-y-1/2 h-9 w-9 rounded-full border border-primary/50 bg-transparent text-primary transition-all duration-300 hover:bg-primary hover:text-primary-foreground hover:shadow-[0_0_15px_hsl(var(--primary))]" disabled={isLoading || (!input.trim() && !photoDataUri)}>
                            <Send size={18} />
                        </Button>
                    </div>
                </form>
            </div>
        </div>
    );
};

export default ChatMode;
